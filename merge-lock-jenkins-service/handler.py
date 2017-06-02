import logging
import json
import os
import sys

here = os.path.dirname(os.path.realpath(__file__))
sys.path.append(os.path.join(here, './vendored'))
sys.path.append(os.path.join(here, './commons'))
from jenkinsapi.jenkins import Jenkins
from commons import publish_to_sns

logger = logging.getLogger()
logger.setLevel(logging.INFO)

from dotenv import load_dotenv
dotenv_path = os.path.join(os.path.dirname(__file__), '.env')
load_dotenv(dotenv_path)

stage = os.environ.get("STAGE")
region = os.environ.get("REGION")
account_id = os.environ.get("ACCOUNT_ID")
DEV_JENKINS_SERVICE_API_ID = os.environ.get("DEV_JENKINS_SERVICE_API_ID")


def pipelineTriggerFunction(event, context):
    logger.info("Pipeline Trigger function invoked with event: %s" % event)
    received_data = json.loads(event['Records'][0]['Sns']['Message'])
    queue = json.loads(received_data['queue'])
    if (len(queue) == 0):
        logger.warn("New Top Listener invoked with empty queue")
        return

    branch = queue[0]['branch']
    return_url = "https://%s.execute-api.eu-west-1.amazonaws.com/%s/mergelock-jenkins/status" % (DEV_JENKINS_SERVICE_API_ID, stage)
    logger.info("Branch %s will be send to Jenkins" % branch)
    jenkins_client = Jenkins('http://jenkins.secretescapes.com:9090', 'mario.martinez@secretescapes.com', '40116ff0cc3575a7ee72e889b05effb3')
    jenkins_client['merge-to-master'].invoke(build_params={'BRANCH_TO_MERGE': branch, 'NOTIFICATION_ENDPOINT': return_url})  
    return


def statusUpdateFunction(event, context):
	try:
		logger.info("Status Update function invoked with event: %s" % event)
		state = json.loads(event['body'])['state']
		branch = json.loads(event['body'])['branch']
		url = json.loads(event['body'])['url']

		logger.info("%s %s %s" % (state, branch, url))

		message = _create_message(state, branch, url)
		if message:
			payload = {'text': message}
			publish_to_sns.publish(stage, "pipeline", account_id, region, payload)
	
	except Exception as e:
		logger.error(e)
	
	return {
		"statusCode": 200
	}


def _create_message(state, branch, url):
	if state == 'START':
		return ":slightly_smiling_face: Hey, I thought it would be good to merge master into *%s* and run tests. You can check the status here: %s" % (branch, url)
	elif state == 'START_MERGE':
		return
	elif state == 'FAILURE_MERGE':
		return ":disappointed: Unfortunately, I couldn't merge master into *%s*, you can check the status here: %s" % (branch, url)
	elif state == 'START_TEST':
		return ":grinning: Good news! I could merge master into *%s* and now I will run tests. You can check the status here: %s" % (branch, url)
	elif state == 'FAILURE_TEST':
		return ":hushed: It seems some tests failed for *%s*, please check them here: %s" % (branch, url)
	elif state == 'SUCCESS':
		return ":white_check_mark: All tests are passing for *%s* :smile:! you can check the results here: %s" % (branch, url)
	elif state == 'FAILURE_ABNORMAL':
		return ":cold_sweat: Something went wrong for *%s*, please check here: %s" % (branch, url)
