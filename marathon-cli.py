import os
import sys
import json
import time
import requests
import logging

from marathon import MarathonClient, MarathonApp, MarathonHttpError, MarathonError

STDOUT_URL = "{}/files/read.json?path=/opt/mesos/slaves/{}/frameworks/{}/executors/{}/runs/{}/stdout"
STDERR_URL = "{}/files/read.json?path=/opt/mesos/slaves/{}/frameworks/{}/executors/{}/runs/{}/stderr"
OFFSET = "&offset={}&length={}"

def get_task_by_version(client, app_id, version):
    tasks = client.list_tasks(app_id=marathon_app_id)
    new_task = None
    for task in tasks:
        # print task
        if task.version == version:
            new_task = task
    return new_task

def print_file_chunk(url, offset, auth):
    length = requests.get(url, auth=auth, verify=False).json()['offset'] - offset
    offset_params = OFFSET.format(offset, length)
    #stdout_url = STDOUT_URL_OFFSET_LENGTH.format(agent_hostname, new_task.slave_id, framework_id, new_task.id, container_id, stdout_offset, stdout_length)
    data = requests.get(url+offset_params, auth=auth, verify=False).json()['data']
    if data != "":
        for line in data.split('\n')[:-1]:
            print line
    return offset + length

if __name__ == '__main__':
    marathon_urls = os.getenv("MARATHON_URLS", "http://localhost:8080").split(',')
    marathon_app_id = os.getenv("MARATHON_APP_ID", "test-app")
    marathon_user = os.getenv("MARATHON_USER", None)
    marathon_password = os.getenv("MARATHON_PASSWORD", None)
    marathon_force = os.getenv("MARATHON_FORCE_DEPLOY", False)
    marathon_framework_name = os.getenv("MARATHON_FRAMEWORK_NAME", "marathon")
    # This could be 10.0.1.34|https://mesos-agent-1.test.dev,10.0.1.35|https://mesos-agent-2.test.dev...
    mesos_agent_map_string = os.getenv("MESOS_AGENT_MAP", None)
    marathon_app = os.getenv("MARATHON_APP","""
        {
            "id": "/test-app",
            "cmd": "mv *.war apache-tomcat-*/webapps && cd apache-tomcat-* && sed \\"s/8080/$PORT/g\\" < ./conf/server.xml > ./conf/server-mesos.xml && sleep 15 && ./bin/catalina.sh run -config ./conf/server-mesos.xml",
            "cpus": 1,
            "mem": 128,
            "disk": 0,
            "instances": 1,
            "user": "mesagent",
            "uris": [
                "http://mirrors.gigenet.com/apache/tomcat/tomcat-7/v7.0.73/bin/apache-tomcat-7.0.73.tar.gz",
                "https://storage.googleapis.com/google-code-archive-downloads/v2/code.google.com/gwt-examples/Calendar.war"
            ],
            "healthChecks": [
                {
                  "protocol": "HTTP",
                  "path": "/",
                  "portIndex": 0,
                  "gracePeriodSeconds": 300,
                  "intervalSeconds": 60,
                  "timeoutSeconds": 20,
                  "maxConsecutiveFailures": 3
                }
            ],
            "env": {
                "toggle": "true"
            }
        }
    """)

    exit_code = 0
    auth = None
    if marathon_user and marathon_password:
        auth = (marathon_user, marathon_password)

    ### Setup Logging
    logging.basicConfig(level=logging.WARN)

    print "Parsing JSON app definition..."
    app_definition = MarathonApp.from_json(json.loads(marathon_app))

    try:
        print "Connecting to Marathon..."
        client = MarathonClient(marathon_urls, username=marathon_user, password=marathon_password, verify=False)
    except MarathonError as e:
        print "Failed to connect to Marathon! {}".format(e)
        exit_code = 1
        sys.exit(exit_code)

    print "Deploying application..."
    try:
        app = client.get_app(marathon_app_id)
    except MarathonHttpError:
        response = client.create_app(marathon_app_id, app_definition)
    else:
        response = client.update_app(marathon_app_id, app_definition, force=marathon_force)

    print "New version deployed: ", response['version']

    ### Get newly created Mesos task

    time.sleep(0.5)
    deployments = client.get_app(marathon_app_id).deployments
    new_task = get_task_by_version(client, marathon_app_id, response["version"])
    deployment_id = response["deploymentId"]

    if not new_task:
        print "New task did not start automatically, probably because the application definition did not change, forcing restart..."
        response = None
        for hostname in marathon_urls:
            try:
                headers = {"content-type": "application/json"}
                response = requests.post("{}/v2/apps/{}/restart".format(hostname, marathon_app_id), auth=auth, verify=False, headers=headers)
            except requests.exceptions.ConnectionError as e:
                print "Marathon connection error, ignoring: {}".format(e)
                pass
            else:
                break

        time.sleep(2)
        new_task = get_task_by_version(client, marathon_app_id, response.json()["version"])
        deployment_id = response.json()["deploymentId"]
        print "New version created by restart: {}".format(response.json()["version"])

    ### Get Framework ID

    marathon_info = client.get_info()
    framework_id = marathon_info.framework_id

    ### Query Mesos API to discover Container ID
    agent_hostname = new_task.host
    mesos_agent_map = {}
    if mesos_agent_map_string:
        for mapping in mesos_agent_map_string.split(','):
            mapping = mapping.split('|')
            mesos_agent_map[mapping[0]] = mapping[1]
        agent_hostname = mesos_agent_map[agent_hostname]
    else:
        agent_hostname = "http://{}:5051".format(agent_hostname)

    mesos_tasks = requests.get("{}/state.json".format(agent_hostname), auth=auth, verify=False)
    marathon_framework = None
    container_id = None

    # TODO: User framework_id instead of marathon_framework_name
    for framework in mesos_tasks.json()['frameworks']:
        if framework['name'] == marathon_framework_name:
            marathon_framework = framework
            break

    if not marathon_framework:
        print "ERROR: Marathon Framework not discoverable via Mesos API."

    for executor in framework['executors']:
        if executor['source'] == new_task.id:
            container_id = executor['container']
            break

    if not container_id:
        print "ERROR: Executor for task {} not found.".format(new_task.id)

    ### Stream STDOUT and STDERR from Mesos until the deployment has completed

    stdout_offset = 0
    stderr_offset = 0
    done = False

    while not done:
        deployments = client.get_app(marathon_app_id).deployments

        if deployments == []:
            time.sleep(3)
            done = True
        # TODO: The deployment does not get replaced on failure, but rather
        # restarts and tries again.  So we need to retrieve the task itself,
        # and check if it has reached TASK_FAILED status
        # else:
        #     deployment_found = False
        #     for deployment in deployments:
        #         if deployment.id == deployment_id:
        #             deployment_found = True
        #
        #     # If our original deployment no longer exists, then it failed.
        #     if not deployment_found:
        #         time.sleep(5)
        #         done = True
        #         exit_code = 1

        # time.sleep(0.5)

        ### Get STDOUT

        stderr_url = STDERR_URL.format(agent_hostname, new_task.slave_id, framework_id, new_task.id, container_id)
        stderr_offset = print_file_chunk(stderr_url, stderr_offset, auth)

        ### Get STDERR

        stdout_url = STDOUT_URL.format(agent_hostname, new_task.slave_id, framework_id, new_task.id, container_id)
        stdout_offset = print_file_chunk(stdout_url, stdout_offset, auth)

        # Small rate limiting factor
        time.sleep(0.1)

    print "All deployments completed sucessfully!"
    sys.exit(exit_code)
