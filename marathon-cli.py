import os
import sys
import json
import time
import requests
import logging

from marathon import MarathonClient, MarathonApp, MarathonHttpError, MarathonError

STDOUT_URL = "http://{}:5051/files/read.json?path=/opt/mesos/slaves/{}/frameworks/{}/executors/{}/runs/{}/stdout"
STDOUT_URL_OFFSET = "http://{}:5051/files/read.json?path=/opt/mesos/slaves/{}/frameworks/{}/executors/{}/runs/{}/stdout&offset={}"
STDOUT_URL_OFFSET_LENGTH = "http://{}:5051/files/read.json?path=/opt/mesos/slaves/{}/frameworks/{}/executors/{}/runs/{}/stdout&offset={}&length={}"
STDERR_URL = "http://{}:5051/files/read.json?path=/opt/mesos/slaves/{}/frameworks/{}/executors/{}/runs/{}/stderr"
STDERR_URL_OFFSET = "http://{}:5051/files/read.json?path=/opt/mesos/slaves/{}/frameworks/{}/executors/{}/runs/{}/stderr&offset={}"
STDERR_URL_OFFSET_LENGTH = "http://{}:5051/files/read.json?path=/opt/mesos/slaves/{}/frameworks/{}/executors/{}/runs/{}/stderr&offset={}&length={}"

def get_task_by_version(client, app_id, version):
    tasks = client.list_tasks(app_id=marathon_app_id)
    new_task = None
    for task in tasks:
        # print task
        if task.version == version:
            new_task = task
    return new_task

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
            "env": {
                "toggle": "true"
            }
        }
    """)

    ### Setup Logging
    logging.basicConfig(level=logging.INFO)

    print "Parsing JSON app definition..."
    app_definition = MarathonApp.from_json(json.loads(marathon_app))

    try:
        print "Connecting to Marathon..."
        client = MarathonClient(marathon_urls, username=marathon_user, password=marathon_password, verify=False)
    except MarathonError as e:
        print "Failed to connect to Marathon! {}".format(e)
        sys.exit(1)

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
    time.sleep(0.5)
    new_task = get_task_by_version(client, marathon_app_id, response["version"])

    auth = None
    if marathon_user and marathon_password:
        auth = (marathon_user, marathon_password)

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

        print response.json()
        time.sleep(1)
        new_task = get_task_by_version(client, marathon_app_id, response.json()["version"])
        print "New version created by restart: {}".format(response.json()["version"])

    ### Get Framework ID

    marathon_info = client.get_info()
    framework_id = marathon_info.framework_id

    ### Query Mesos API to discover Container ID
    hostname = new_task.host
    mesos_agent_map = {}
    if mesos_agent_map_string:
        for mapping in mesos_agent_map_string.split(','):
            mapping = mapping.split('|')
            mesos_agent_map[mapping[0]] = mapping[1]
        hostname = mesos_agent_map[hostname]
    else:
        hostname = "http://{}:5051".format(hostname)

    mesos_tasks = requests.get("{}/state.json".format(hostname), auth=auth, verify=False)
    print mesos_tasks
    
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
    stdout_length = None
    stderr_offset = 0
    stderr_length = None
    done = False

    while not done:
        deployments = client.get_app(marathon_app_id).deployments
        if deployments == []:
            time.sleep(5)
            done = True

        time.sleep(0.5)

        ### Get STDOUT

        # If stdout_length is set, read in the data then unset it, so the next run will retrieve the new length.
        if stdout_length:
            stdout_url = STDOUT_URL_OFFSET_LENGTH.format(new_task.host, new_task.slave_id, framework_id, new_task.id, container_id, stdout_offset, stdout_length)
            stdout_offset += stdout_length
            stdout = requests.get(stdout_url, auth=auth, verify=False)
            if stdout.json()['data'] != "":
                stdout_lines = stdout.json()['data'].split('\n')
                for line in stdout_lines[:-1]:
                    print "{}".format(line)
            stdout_length = None
        else:
            # This retrieves the current data length, since offset and length are not specified
            stdout_url = STDOUT_URL.format(new_task.host, new_task.slave_id, framework_id, new_task.id, container_id)
            stdout = requests.get(stdout_url, auth=auth, verify=False)
            stdout_length = stdout.json()['offset']
            stdout_length -= stdout_offset

        ### Get STDERR

        # Move the offset forward to the previous length read, if any
        if stderr_length:
            stderr_url = STDERR_URL_OFFSET_LENGTH.format(new_task.host, new_task.slave_id, framework_id, new_task.id, container_id, stderr_offset, stderr_length)
            stderr_offset += stderr_length
            stderr = requests.get(stderr_url, auth=auth, verify=False)
            if stderr.json()['data'] != "":
                stderr_lines = stderr.json()['data'].split('\n')
                for line in stderr_lines[:-1]:
                    print "{}".format(line)
            stderr_length = None
        else:
            stderr_url = STDERR_URL.format(new_task.host, new_task.slave_id, framework_id, new_task.id, container_id)
            stderr = requests.get(stderr_url, auth=auth, verify=False)
            stderr_length = stderr.json()['offset']
            stderr_length -= stderr_offset

    print "All deployments completed sucessfully!"
