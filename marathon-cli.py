import os
import sys
import json
import time
import requests
import logging
import pprint

from marathon import (MarathonClient, MarathonApp, MarathonHttpError,
                      MarathonError)

TASK_STATUS_URL = "{}/tasks.json"
STDOUT_URL = "{}/files/read.json?path=/opt/mesos/slaves/{}/frameworks/{}/executors/{}/runs/{}/stdout"
STDERR_URL = "{}/files/read.json?path=/opt/mesos/slaves/{}/frameworks/{}/executors/{}/runs/{}/stderr"
OFFSET = "&offset={}&length={}"

def get_task_by_version(client, app_id, version):
    """
    Gets the Mesos task using the Marathon version of the deployment.
    """
    logging.debug("Attempting to get task for app version {}".format(version))
    tasks = client.list_tasks(app_id=marathon_app_id)
    new_task = None
    for task in tasks:
        logging.debug("Found task: {}".format(task))
        if task.version == version:
            logging.debug("Task with version {} found!".format(version))
            new_task = task
    if not new_task:
        logging.debug("Failed to find task for version {}".format(version))
    return new_task

def print_file_chunk(url, offset, auth):
    """
    Takes a URL pointing to a Mesos file, and an offset, and prints
    the file contents from offset to the end, then returns the new offset.
    """
    response = requests.get(url, auth=auth, verify=False)
    try:
        length = response.json()['offset'] - offset
    except ValueError:
        logging.debug("Invalid JSON response received: {} from URL {}, skipping...".format(response, url))
        length = 0
    
    offset_params = OFFSET.format(offset, length)
    response = requests.get(url+offset_params, auth=auth, verify=False)
    try:
        data = response.json()['data']
    except ValueError:
        logging.debug("Invalid JSON response received: {} from URL {}, skipping...".format(response, url))
        data = ""
    
    if data != "":
        for line in data.split('\n')[:-1]:
            logging.info("CONTAINER LOG: {}".format(line))

    return offset + length

if __name__ == '__main__':
    """
    This script reads in values from environment variables, then deploys a
    Marathon application as defined in MARATHON_APP.

    MARATHON_URLS:              One or more URL's to try when communication with
                                Marathon, separated by commas.
    MARATHON_APP_ID:            The identifier of the application in Marathon.
    MARATHON_USER:              The user to use, if needed.
    MARATHON_PASSWORD:          The password to use, if needed.
    MARATHON_FORCE_DEPLOY:      Use the Force, if necessary (i.e. when a
                                deployment is failing.)
    MARATHON_FRAMEWORK_NAME:    The name of the framework (usually 'marathon')
    MARATHON_APP:               The JSON formatted app definition.
    MARATHON_RETRIES:           The number of task failures to tolerate.
    MESOS_AGENT_MAP:            This is used when Mesos is behind a proxy.  The
                                API will return the Mesos Agent IP address,
                                but that may need to be mapped to a URL.  The
                                map is defined like:
                                10.0.1.34|https://mesos.test.dev,...
    MESOS_MASTER_URLS:          One or more urls for Mesos communication,
                                separated by commas.
    """

    marathon_urls = os.getenv("MARATHON_URLS", "http://localhost:8080").split(',')
    marathon_app_id = os.getenv("MARATHON_APP_ID", "test-app")
    marathon_user = os.getenv("MARATHON_USER", None)
    marathon_password = os.getenv("MARATHON_PASSWORD", None)
    marathon_force = True if os.getenv("MARATHON_FORCE_DEPLOY", "false") == "true" else False
    marathon_framework_name = os.getenv("MARATHON_FRAMEWORK_NAME", "marathon")
    marathon_retries = int(os.getenv("MARATHON_RETRIES", 3))
    log_level = os.getenv("MARATHON_LOGLEVEL", 'info')
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
            ]
        }
    """)
    mesos_agent_map_string = os.getenv("MESOS_AGENT_MAP", None)
    mesos_master_urls = os.getenv("MESOS_MASTER_URLS", "http://localhost:5050").split(',')

    pp = pprint.PrettyPrinter(depth=2)

    exit_code = 0
    auth = None
    if marathon_user and marathon_password:
        auth = (marathon_user, marathon_password)

    ### Setup Logging
    logging.basicConfig(format="%(levelname)-8s %(message)s", level=getattr(logging, log_level.upper()))
    logging.getLogger('marathon').setLevel(logging.WARN) # INFO is too chatty

    logging.info("Parsing JSON app definition...")
    app_definition = MarathonApp.from_json(json.loads(marathon_app))

    try:
        logging.info("Connecting to Marathon...")
        client = MarathonClient(marathon_urls, username=marathon_user, password=marathon_password, verify=False)
    except MarathonError as e:
        logging.error("Failed to connect to Marathon! {}".format(e))
        exit_code = 1
        sys.exit(exit_code)

    logging.info("Deploying application...")
    try:
        app = client.get_app(marathon_app_id)
    except MarathonHttpError:
        response = client.create_app(marathon_app_id, app_definition)
        version = response.version
        depolyment_id = response.deployments[0].id
    else:
        response = client.update_app(marathon_app_id, app_definition, force=marathon_force)
        version = response['version']
        deployment_id = response['deploymentId']

    logging.info("New version deployed: {}".format(version))

    if app_definition.instances == 0:
        logging.info("Deactivated application by setting instances to 0, deployment complete.")
        exit_code = 0
        sys.exit(exit_code)

    ### Get newly created Mesos task

    time.sleep(5)
    new_task = get_task_by_version(client, marathon_app_id, version)

    if not new_task:
        logging.warn("New task did not start automatically, probably because the application definition did not change, forcing restart...")
        response = None
        for hostname in marathon_urls:
            try:
                headers = {"content-type": "application/json"}
                force_string = "true" if marathon_force else "false"
                response = requests.post("{}/v2/apps/{}/restart?force={}".format(
                    hostname, marathon_app_id, force_string), auth=auth, verify=False,
                    headers=headers)
            except requests.exceptions.ConnectionError as e:
                logging.warn("Marathon connection error, ignoring: {}".format(e))
                pass
            else:
                break

        if response.status_code != 200:
            logging.error("Failed to force application restart, received response {}, exiting...".format(response.text))
            exit_code = 1
            sys.exit(exit_code)

        attempts = 0
        while not new_task and attempts < 10:
            time.sleep(2)
            new_task = get_task_by_version(client, marathon_app_id, response.json()["version"])
            attempts += 1

        if not new_task:
            logging.error("Unable to retrieve new task from Marathon, there may be a communication failure with Mesos.")
            exit_code = 1
            sys.exit(exit_code)

        deployment_id = response.json()["deploymentId"]
        logging.info("New version created by restart: {}".format(response.json()["version"]))

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

    logging.info('SSH command: ssh -t {ip} "cd /opt/mesos/slaves/*/frameworks/*/executors/{run}/runs/latest; exec \\$SHELL -l"'.format(ip=new_task.host, ex=new_task.app_id, run=new_task.id))

    mesos_tasks = requests.get("{}/state.json".format(agent_hostname), auth=auth, verify=False)
    marathon_framework = None
    container_id = None

    # TODO: User framework_id instead of marathon_framework_name
    try:
        mesos_tasks = mesos_tasks.json()
    except ValueError as e:
        logging.error("Error {} from response {}".format(e, mesos_tasks.text))
        logging.error("Deployment may have started, but cannot confirm with Mesos.")
        exit_code = 1
        sys.exit(exit_code)

    for framework in mesos_tasks['frameworks']:
        if framework['name'] == marathon_framework_name:
            marathon_framework = framework
            break

    if not marathon_framework:
        logging.error("Marathon Framework not discoverable via Mesos API.")

    for executor in framework['executors']:
        if executor['source'] == new_task.id:
            container_id = executor['container']
            break

    if not container_id:
        logging.error("Executor for task {} not found.".format(new_task.id))

    ### Stream STDOUT and STDERR from Mesos until the deployment has completed
    logging.info("Streaming logs from Mesos...\n")

    attempts = 0

    # Allow up to marathon_retries tasks to fail
    while attempts < marathon_retries:

        stdout_offset = 0
        stderr_offset = 0
        done = False
        failed = False

        # Stream stdout and stderr to the console
        while not done:
            deployments = client.get_app(marathon_app_id).deployments

            if deployments == []:
                logging.debug("No deployments remaining, set done=True.")
                time.sleep(3)
                done = True
            else:
                mesos_tasks = None
                logging.debug("Getting Mesos task state...")

                for host in mesos_master_urls:
                    logging.debug("Trying Mesos host {}...".format(host))
                    try:
                        response = requests.get(TASK_STATUS_URL.format(host), auth=auth, verify=False, timeout=1)
                    except requests.exceptions.ConnectionError:
                        logging.debug("Failed to connect to Mesos host {}, trying next host...".format(host))
                        continue
                    except requests.exceptions.ReadTimeout:
                        logging.debug("Read timeout from Mesos host {}, trying next host...".format(host))
                        continue

                    if response.status_code == 200:
                        mesos_tasks = response.json()
                        break
                    else:
                        logging.warn("Response code != 200: {}".format(pp.pprint(response)))

                if mesos_tasks:
                    for task in mesos_tasks['tasks']:
                        #print task
                        if task['id'] == new_task.id:
                            if task['state'] in ["TASK_FAILED", "TASK_KILLED", "TASK_FINISHED"]:
                                logging.warn("task failed: {}".format(pp.pprint(task)))
                                failed = True
                                done = True

                else:
                    logging.warn("Failed to connect to Mesos API, task status not available.")

            ### Get STDOUT

            stderr_url = STDERR_URL.format(agent_hostname, new_task.slave_id, framework_id, new_task.id, container_id)
            stderr_offset = print_file_chunk(stderr_url, stderr_offset, auth)

            ### Get STDERR

            stdout_url = STDOUT_URL.format(agent_hostname, new_task.slave_id, framework_id, new_task.id, container_id)
            stdout_offset = print_file_chunk(stdout_url, stdout_offset, auth)

            # Small rate limiting factor
            time.sleep(0.1)

        if failed:
            logging.warn("Deployment task failed, trying again...")
            attempts += 1
        else:
            break

    print()

    # Wait for logs to print
    time.sleep(5)

    logging.info("End of log stream from Mesos.")

    if failed:
        logging.error("Failure deploying new app configuration, aborting deployment!")

        for hostname in marathon_urls:
            try:
                headers = {"content-type": "application/json"}
                response = requests.delete("{}/v2/deployments/{}?force=true".format(
                    hostname, deployment_id), auth=auth, verify=False,
                    headers=headers)
            except requests.exceptions.ConnectionError as e:
                logging.warn("Marathon connection error, ignoring: {}".format(e))
                pass
            else:
                break

        if response.status_code in [200, 202]:
            logging.warn("Successfully cancelled failed deployment.")
        else:
            logging.error("Failed to force stop deployment: {}, you may need to try again with MARATHON_FORCE_DEPLOY=true, exiting...".format(response.text))

        exit_code = 1
    else:
        logging.info("All deployments completed sucessfully!")
    sys.exit(exit_code)
