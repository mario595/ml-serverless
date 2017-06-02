import json
import logging
import os
import sys
from commons import publish_to_sns


here = os.path.dirname(os.path.realpath(__file__))
sys.path.append(os.path.join(here, './vendored'))
import requests

from dotenv import load_dotenv
dotenv_path = os.path.join(os.path.dirname(__file__), '.env')
load_dotenv(dotenv_path)

stage = os.environ.get("STAGE")
region = os.environ.get("REGION")
user_service_api_id = os.environ.get("%s_USER_SERVICE_API_ID" % stage.upper())
ACCOUNT_ID = os.environ.get("ACCOUNT_ID")

logger = logging.getLogger()
logger.setLevel(logging.INFO)

def push(event, context):
    try:
        params = json.loads(event.get('body'))
        branch_name = params['ref']
        author_username = params['pusher']['name']
        logger.info("Push invoked for branch %s, by author %s" % (branch_name, author_username))

        if branch_name == 'refs/heads/master':
            response = requests.get("https://%s.execute-api.%s.amazonaws.com/%s/user-service/user/reverse/%s" % (user_service_api_id, region, stage, author_username))
            if response.status_code == 200:
                username = response.json()[0]['username']
                _publish_push(username)
            else:
                _publish_push(author_username)
        return {
            "statusCode": 200
        }
    except Exception as e:
        logging.exception(e)
        return {
            "statusCode": 500
        }


def _publish_push(username):
    try:
        payload = {'username': username}
        publish_to_sns.publish(stage, "push", ACCOUNT_ID, region, payload)
    except Exception as e:
        logger.error("Exception publishing: %s" % e)