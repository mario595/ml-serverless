import boto3
import botocore
import time
import logging
import json
from commons import format_utils
import os
import sys



here = os.path.dirname(os.path.realpath(__file__))
sys.path.append(os.path.join(here, "./vendored"))

from dotenv import load_dotenv
dotenv_path = os.path.join(os.path.dirname(__file__), '.env')
load_dotenv(dotenv_path)

import requests

logger = logging.getLogger()
logger.setLevel(logging.INFO)

stage = os.environ.get("STAGE")
region = os.environ.get("REGION")
REPLIER_LAMBDA_NAME = os.environ.get("REPLIER_LAMBDA_NAME")

user_service_api_id = os.environ.get("%s_USER_SERVICE_API_ID" % stage.upper())
queue_service_api_id = os.environ.get("%s_QUEUE_SERVICE_API_ID" % stage.upper())
status_service_api_id = os.environ.get("%s_STATUS_SERVICE_API_ID" % stage.upper())
SLACK_TOKEN = os.environ.get("%s_SLACK_TOKEN" % stage.upper())

unknown_error_message = ":dizzy_face: Something went really wrong, sorry"

_lambda = boto3.client('lambda')

def merge_lock(event, context):
    try:
        logger.info("mergeLock invoke with event: %s" % event)
        params = event.get('body')
        _validate(params['token'])
        text = params.get('text').split()
        response_url = params.get('response_url')
        logger.info("Command received: %s" % text)
        payload = {'text' : text, 'response_url' : response_url, 'username':params.get('user_name'), 'user_id': params.get('user_id')}

        _lambda.invoke(
            FunctionName=REPLIER_LAMBDA_NAME,
            InvocationType='Event',
            Payload=json.dumps(payload))
        
        return {"text":'Got it!'}
    except Exception as e:
        logger.error("Exception: %s" % e)
        return {"text": unknown_error_message}

def dispatcher(event, context):

    try:
        logger.info("Dispatcher invoked with event: %s" % event)
        text = event['text']
        response_url = event['response_url']
        slack_username = event['username']
        user_id = event['user_id']

        response = "unrecognized command, please try one of these:\n/lock list\n/lock add [username] [branch]\n/lock remove [username]\n/lock back [username]\n/lock register me [github username]\n/lock window open\n/lock window close\n(tip: you can use _me_ instead of your username)"

        if len(text) == 1 and text[0].lower() == 'list':
            response = _list_request_handler()

        elif len(text) == 2 and text[0].lower() == 'remove':
            username = _resolve_username(text[1], slack_username, user_id)
            response = _remove_request_handler(username)

        elif len(text) == 2 and text[0].lower() == 'back':
            username = _resolve_username(text[1], slack_username, user_id)
            response = _back_request_handler(username)

        elif len(text) > 1 and text[0].lower() == 'window':
            action = text[1]
            if (action.lower() == "open"):
                response = _open_window_handler()
            elif (action.lower() == "close"):
                response = _close_window_handler()
        elif len(text) == 2 and text[0].lower() == 'add':
            response = "%s\n*remember that you have to provide a branch name*: `/lock add [username] [branch]`" % response

        elif len(text) == 3 and text[0].lower() == 'add':
            username = _resolve_username(text[1], slack_username, user_id)
            branch = text[2]
            response = _add_request_handler(username, branch)

        elif len(text) == 3 and text[0].lower() == 'register':
            username = _resolve_username(text[1], slack_username, user_id)
            githubUsername = text[2]
            response = _register_request_handler(username, githubUsername)

    except Exception as e:
        logger.error("Exception: %s" % e)
        response = unknown_error_message

    logger.info("Command response: %s" % response)
    headers = {'content-type': 'application/json'}
    payload = {'text': response}
    requests.post(response_url, data=json.dumps(payload), headers=headers)
    

def _open_window_handler():
    url = "https://%s.execute-api.%s.amazonaws.com/%s/window/open" % (status_service_api_id, region, stage)
    response = requests.get(url)
    if response.status_code == 200:
        return "Release window is now open"
    else:
        logger.error("Status code receive: %i" % response.status_code)   
        return unknown_error_message

def _close_window_handler():
    url = "https://%s.execute-api.%s.amazonaws.com/%s/window/close" % (status_service_api_id, region, stage)
    response = requests.get(url)
    if response.status_code == 200:
        return "Release window is now close"
    else:
        logger.error("Status code receive: %i" % response.status_code)   
        return unknown_error_message

def _resolve_username(command_username, requester_username, requester_user_id):
    if command_username.lower() == 'me':
        return "<@%s|%s>"%(requester_user_id,requester_username)
    else:
        return command_username

def _back_request_handler(username):
    response = requests.post("https://%s.execute-api.%s.amazonaws.com/%s/mergelock/back" % (queue_service_api_id, region, stage), data={'username': username})
    if response.status_code == 200:
        return '%s has been *pulled back* one position in the queue :point_down:' % username
    elif response.status_code == 401:
        return '%s is not in the queue' % username
    elif response.status_code == 402:
        return '%s is already at the bottom of the queue' % username
    else:
        logger.error("Status code receive: %i" % response.status_code)   
        return unknown_error_message


def _register_request_handler(username, githubUsername):
    logger.info("Register request for %s - %s"%(username, githubUsername))
    response = requests.put("https://%s.execute-api.%s.amazonaws.com/%s/user-service/user" % (user_service_api_id, region, stage), data={'username': username, 'githubUsername':githubUsername})
    if response.status_code == 200:
        return '%s has been *registered* with the github username %s' % (username, githubUsername)
    else:   
        logger.error("Status code receive: %i" % response.status_code)   
        return unknown_error_message

def _remove_request_handler(username):
    response = requests.post("https://%s.execute-api.%s.amazonaws.com/%s/mergelock/remove" % (queue_service_api_id, region, stage), data={'username': username})
    if response.status_code == 200:
        return '%s has been *removed* from the queue' % username
    elif response.status_code == 401:
        return '%s is not in the queue' % username
    else:
        logger.error("Status code receive: %i" % response.status_code)   
        return unknown_error_message

    return response.text

def _add_request_handler(username, branch):
    response = requests.post("https://%s.execute-api.%s.amazonaws.com/%s/mergelock/add" % (queue_service_api_id, region, stage), data={'username': username, 'branch': branch})
    logger.info ("Status code: %d" % response.status_code)
    if response.status_code == 200:
        return '%s has been *added* to the queue :point_right:' % username
    elif response.status_code == 401:
        return '%s *was already* in the queue' % username
    elif response.status_code == 402:
        return ':warning: %s is not *registered*' % username
    else:
        logger.error("Status code receive: %i" % response.status_code)   
        return unknown_error_message

def _list_request_handler():
    url = "https://%s.execute-api.%s.amazonaws.com/%s/mergelock/list" % (queue_service_api_id, region, stage)
    response = requests.get(url)
    if response.status_code == 200:
        return format_utils.format_queue(response.json()['queue'])
    else:
        logger.error("Status code received: %i" % response.status_code)
        logger.error("Error received: %i" % response.json()['queue'])
        return {'text': unknown_error_message}

def _validate(token):
    if (token != SLACK_TOKEN):
        raise Exception("Incorrect token")