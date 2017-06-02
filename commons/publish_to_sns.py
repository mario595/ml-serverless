import boto3
import logging
import json

sns = boto3.client('sns')
logger = logging.getLogger()
logger.setLevel(logging.INFO)

def publish(stage, topic, account_id, region, payload):
    topicArn = 'arn:aws:sns:%s:%s:%s-%s' % (region, account_id, stage, topic)
    logger.info("Publish message %s in topic %s" % (payload, topicArn))
    try:
        response = sns.publish(
            TopicArn= topicArn,
            Message= json.dumps(payload))
        logger.info("Publish response: %s" % response)
    except Exception as e:
        logger.error("Exception publishing in topic %s: %s" % (topic, e))