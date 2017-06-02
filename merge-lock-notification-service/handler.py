import json
import os
import sys
import logging
import boto3
from commons import format_utils

here = os.path.dirname(os.path.realpath(__file__))
sys.path.append(os.path.join(here, './vendored'))

import requests

from dotenv import load_dotenv
dotenv_path = os.path.join(os.path.dirname(__file__), '.env')
load_dotenv(dotenv_path)

logger = logging.getLogger()
logger.setLevel(logging.INFO)
_lambda = boto3.client('lambda')
stage = os.environ.get("STAGE")
region = os.environ.get("REGION")
SLACK_WEBHOOK_URL = os.environ.get("%s_SLACK_WEBHOOK_URL" % stage.upper())
queue_service_api_id = os.environ.get("%s_QUEUE_SERVICE_API_ID" % stage.upper())

def user_added_listener(event, context):
    logger.info("User Added Listener invoked with event: %s" % event)

    received_data = json.loads(event['Records'][0]['Sns']['Message'])
    queue = json.loads(received_data['queue'])

    payload = {'text': "*%s* has been added\n%s" % 
                    (received_data['username'],format_utils.format_queue(queue))}

    _notify_slack(payload)

def new_top_listener(event, context):
    logger.info("New Top Listener invoked with event: %s" % event)
    received_data = json.loads(event['Records'][0]['Sns']['Message'])
    queue = json.loads(received_data['queue'])
    if (len(queue) == 0):
        logger.warn("New Top Listener invoked with empty queue")
        return

    payload = {'text': ":bell:*%s*, you have acquired the merge lock!:bell:\n%s" % (queue[0]['username'], format_utils.format_queue(queue))}
    _notify_slack(payload)

def push_closed_window_listener(event, context):
    logger.info("Unauthorized push (Window closed) invoked with event: %s" % event)
    received_data = json.loads(event['Records'][0]['Sns']['Message'])

    payload = {'text': ":rotating_light:*%s* has merged while the release window is closed:rotating_light:" % 
                    (received_data['username'])}
    _notify_slack(payload)

def unauthorized_push_listener(event, context):
    logger.info("Unauthorized push invoked with event: %s" % event)

    received_data = json.loads(event['Records'][0]['Sns']['Message'])
    queue = json.loads(received_data['queue'])

    payload = {'text': ":rotating_light:*%s* has merged without merge lock:rotating_light:\n%s" % 
                    (received_data['username'], format_utils.format_queue(queue))}
    _notify_slack(payload)


def status_change_listener(event, context):
    logger.info("Status Change invoked with event: %s" % event)

    received_data = json.loads(event['Records'][0]['Sns']['Message'])
    new_status = received_data['new_status']
    if new_status == "OPEN":
        text = ":trainonfire:The release window is now OPEN!:trainonfire:"
    
    elif new_status == "CLOSE":
        text = ":construction:The release window is now CLOSE!:construction:"

    url = "https://%s.execute-api.%s.amazonaws.com/%s/mergelock/list" % (queue_service_api_id, region, stage)
    response = requests.get(url)

    if response.status_code == 200:
        logger.info("RESPONSE JSON: %s" %response.json())
        queue = format_utils.format_queue(response.json()['queue'])
        text = text + "\n%s"%(queue)

    payload = {'text': text}
    _notify_slack(payload)

def pipeline_status(event, context):
     logger.info("Pipeline Status invoked with event: %s" % event)
     received_data = json.loads(event['Records'][0]['Sns']['Message'])
     _notify_slack({'text': received_data['text']})

def _notify_slack(payload):
    url = SLACK_WEBHOOK_URL
    payload = {'text': payload['text']}
    headers = {'content-type': 'application/json'}

    logger.info("This payload will be sent do the webhook: %s" % payload)

    response = requests.post(url, data=json.dumps(payload), headers = headers)

    logger.info("Notification sent with statusCode: %s" % response.status_code)