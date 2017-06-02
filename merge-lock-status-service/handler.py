import json
import boto3
import os
import logging
import time
import sys
from boto3.dynamodb.conditions import Key, Attr
from commons import publish_to_sns

here = os.path.dirname(os.path.realpath(__file__))
sys.path.append(os.path.join(here, './vendored'))
sys.path.append(os.path.join(here, './commons'))

from dotenv import load_dotenv
dotenv_path = os.path.join(os.path.dirname(__file__), '.env')
load_dotenv(dotenv_path)

stage = os.environ.get("STAGE")
region = os.environ.get("REGION")
ACCOUNT_ID = os.environ.get("ACCOUNT_ID")

logger = logging.getLogger()
logger.setLevel(logging.INFO)

CLOSE_EVENT = "CLOSE"
OPEN_EVENT = "OPEN"

def open(event, context):
    logger.info("Invoked open with event: %s" % event)
    try:
        previous = _get_status()
        _insert_status_event(OPEN_EVENT)
        current = _get_status()
        _notify_if_change(previous, current)

    except Exception as e: 
        logger.error("Exception: %s" % e)
        return _responseError(500, "Unknown error")

    return {
        "statusCode": 200
    }

def close(event, context):
    logger.info("Invoked open with event: %s" % event)
    try:
        previous = _get_status()
        _insert_status_event(CLOSE_EVENT)
        current = _get_status()
        _notify_if_change(previous, current)

    except Exception as e: 
        logger.error("Exception: %s" % e)
        return _responseError(500, "Unknown error")

    return {
        "statusCode": 200
    }

def status(event, context):
    logger.info("Invoked status with event: %s" % event)
    try:
        status = _get_status()

        return {
            "statusCode": 200,
            "body": json.dumps({"status": status})
        }
    except Exception as e:
        logger.error("Exception: %s" % e)
        return _responseError(500, "Unknown error")

def pushListener(event, context):
    logger.info("Invoked push listener with event: %s" % event)
    if _get_status() == "CLOSE":
        logger.info("Push while window is closed")
        received_data = json.loads(event['Records'][0]['Sns']['Message'])
        username = received_data['username']
        payload = {'username': username}
        publish_to_sns.publish(stage, "push_closed_window_listener", ACCOUNT_ID, region, payload)


def _get_status():
    last_open = _retrieve_last_open_event()
    last_closed = _retrieve_last_closed_event()

    return _calculate_status(last_open, last_closed)

def _notify_if_change(previous, current):
    if (previous != current):
        payload = {'new_status': current}
        publish_to_sns.publish(stage, "change_status", ACCOUNT_ID, region, payload)

def _calculate_status(last_open_event, last_closed_event):
    if last_open_event and last_closed_event:
        if last_open_event['timestamp'] > last_closed_event['timestamp']:
            status = "OPEN"
        else:
            status = "CLOSE"
    elif last_open_event and not last_closed_event:
            status = "OPEN"
    else:
            status = "CLOSE"
    return status

def _retrieve_last_event(event_type):
    response = _getTable('statusEvents').query(
        KeyConditionExpression = Key('type').eq(event_type),
        Limit = 1,
        ScanIndexForward = False
    )
    if len(response['Items']) > 0:
        return response['Items'][0]

def _retrieve_last_open_event():
    return _retrieve_last_event(OPEN_EVENT)

def _retrieve_last_closed_event():    
    return _retrieve_last_event(CLOSE_EVENT)


def _insert_status_event(event):
    table = _getTable('statusEvents')
    return table.put_item(
        Item = {
        'type': event,
        'timestamp': int(round(time.time() * 1000))
        }
    )

def _responseError(status_code, error_msg):
    return {
            "statusCode": status_code,
            "body": '{"error": "%s"}' % error_msg
        }

def _getTable(table_name):
    dynamodb = boto3.resource('dynamodb', region_name=region)
    return dynamodb.Table("%s-%s" %(table_name, stage))