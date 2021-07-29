import uuid
import boto3
import json
import logging

from boto3.dynamodb.conditions import Key, Attr
from botocore.exceptions import ClientError

logger = logging.getLogger()
logger.setLevel(logging.INFO)

def find_word(paginator, id, filterExpression):
    """
    Gets a word from DynamoDB Table
    """
    parameters_for_less_that = {
        'TableName': 'Dictionary',
        'FilterExpression': filterExpression,
        'ExpressionAttributeValues': {
            ':x': {'S': id}
        }
    }
    
    page_iterator = paginator.paginate(**parameters_for_less_that)
    
    word = None
    for page in page_iterator:
        for item in page['Items']:
            word = item['word']['S']
            break
        break
    
    logger.debug(f"find_word::word: '{word}'")
    
    return word

def find_random_word(paginator, id):
    """
    Gets a random word from DynamoDB Table
    """
    word = find_word(paginator, id, 'id < :x')
    if word is None:
        word = find_word(paginator, id, 'id >= :x')
    
    logger.info(f"find_random_word::word: '{word}'")
    
    return word

def send_to_connection(data, event):
    """
    Sends a random word to connected client
    """
    gatewayapi = boto3.client("apigatewaymanagementapi",
        endpoint_url = "https://" 
            + event["requestContext"]["domainName"] 
            + "/" 
            + event["requestContext"]["stage"]
    )
    connection_id = event['requestContext']['connectionId']
    response = gatewayapi.post_to_connection(
        ConnectionId = connection_id,
        Data = json.dumps(data).encode('utf-8')
    )
    logger.info(f"send_to_connection::response: '{response}'")
    
    return response

def lambda_handler(event, context):
    """
    Sends a random word from DynamoDB Table using WebSockets
    """
    client = boto3.client('dynamodb')
    paginator = client.get_paginator('scan')

    id = str(uuid.uuid4().hex)
    word = find_random_word(paginator, id)
    
    data = {
        "random_word": word
    }

    send_to_connection(data, event)

    return {
        'statusCode': 200,
        'body': json.dumps(data)
    }
