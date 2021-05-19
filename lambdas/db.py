import boto3
from botocore.exceptions import ClientError

# This is only for local testing
def create_tables(dynamodb=None):
    if not dynamodb:
        dynamodb = boto3.resource('dynamodb', endpoint_url="http://localhost:8000")
    dynamodb.create_table(
        TableName='market_books',
        KeySchema=[{ 'AttributeName': 'market_id', 'KeyType': 'HASH' }],
        AttributeDefinitions=[{ 'AttributeName': 'market_id', 'AttributeType': 'S' }],
        ProvisionedThroughput={ 'ReadCapacityUnits': 5, 'WriteCapacityUnits': 5 }
    )
    dynamodb.create_table(
        TableName='bet_market_books',
        KeySchema=[{ 'AttributeName': 'market_id', 'KeyType': 'HASH' }],
        AttributeDefinitions=[{ 'AttributeName': 'market_id', 'AttributeType': 'S' }],
        ProvisionedThroughput={ 'ReadCapacityUnits': 5, 'WriteCapacityUnits': 5 }
    )
    dynamodb.create_table(
        TableName='bet_runs',
        KeySchema=[{ 'AttributeName': 'run_id', 'KeyType': 'HASH' }],
        AttributeDefinitions=[{ 'AttributeName': 'run_id', 'AttributeType': 'S' }],
        ProvisionedThroughput={ 'ReadCapacityUnits': 5, 'WriteCapacityUnits': 5 }
    )


def put_item(request, dynamodb=None):
    if not dynamodb:
        dynamodb = boto3.resource('dynamodb', endpoint_url="http://localhost:8000")
    table = dynamodb.Table(request['table_name'])
    try:
        response = table.put_item(Item=request['item'])
    except ClientError as e:
        print(e.response['Error']['Message'])
    else:
        return request['item']


def get_item(request, dynamodb=None):
    if not dynamodb:
        dynamodb = boto3.resource('dynamodb', endpoint_url="http://localhost:8000")
    table = dynamodb.Table(request['table_name'])
    try:
        response = table.get_item(Key=request['key'])
    except ClientError as e:
        print(e.response['Error']['Message'])
    else:
        return response.get('Item')

def get_items(request, dynamodb):
    if not dynamodb:
        dynamodb = boto3.resource('dynamodb', endpoint_url="http://localhost:8000")
    table = dynamodb.Table(request['table_name'])
    data = None
    try:
        response = table.scan()
    except ClientError as e:
        print(e.response['Error']['Message'])
    else:
        response = table.scan()
        data = response['Items']
        while 'LastEvaluatedKey' in response:
            response = table.scan(ExclusiveStartKey=response['LastEvaluatedKey'])
            data.extend(response['Items'])
    return data

def update_item(request, dynamodb):
    if not dynamodb:
        dynamodb = boto3.resource('dynamodb', endpoint_url="http://localhost:8000")
    table = dynamodb.Table(request['table_name'])
    response = table.update_item(
        Key=request['key'],
        UpdateExpression=request['update_expression'],
        ConditionExpression=request['condition_expression'],
        ExpressionAttributeValues=request['expression_attribute_values'],
        ReturnValues="ALL_NEW"
    )
    return response
