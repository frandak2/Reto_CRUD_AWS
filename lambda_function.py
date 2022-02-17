import boto3
import json
import logging
from joblib import load
import numpy as np
# from custumer_encoder import CustumEncoder

logger = logging.getLogger()
logger.setLevel(logging.INFO)

dynamodb = boto3.resource('dynamodb')
table = dynamodb.Table('db_viviendas')

getMethod = 'GET'
postMethod = 'POST'
patchMethod = 'PATCH'
deleteMethod = 'DELETE'
viviendaMethod = '/vivienda'
testMethod = '/test'

## funcion que construye respuesta tipo json
def buildResponse(statusCode, body=None):
    response = {
        'statusCode': statusCode,
        'headers' : {
            'Content-type': 'application/json'
            # 'Access-Control-Allow-Origin': '*'
        } 
    }
    if body is not None:
        response['body'] = json.dumps(body) #cls=CustumEncoder
    return response

def lambda_handler(event, context):
    logger.info(event)
    httpMethod = event['httpMethod']
    path = event['path']

    if httpMethod == getMethod and path== testMethod:
        response = buildResponse(200)
    elif httpMethod == getMethod and path== viviendaMethod:
        response = getVivienda(event['queryStringParameters']['id_vivienda'])
    elif httpMethod == postMethod and path== viviendaMethod:
        response = saveVivienda(json.loads(event['body']))
    elif httpMethod == patchMethod and path== viviendaMethod:
        requestBody = json.loads(event['body'])
        response = modifyVivienda(requestBody['id_vivienda'],requestBody['updateCol'],requestBody['updateValue'])
    elif httpMethod == deleteMethod and path== viviendaMethod:
        requestBody = json.loads(event['body'])
        response = deleteVivienda(requestBody['id_vivienda'])
    else:
        response = buildResponse(404, 'Not Found')

    return response

def getVivienda(id_vivienda):
    try:
        response = table.get_item(
            Key = {
                'id_vivienda':id_vivienda
            }
        )
        if 'Item' in response:
            return buildResponse(200, response['Item'])
        else:
            return buildResponse(404, {'Message': 'id_vivienda: %s not found' % id_vivienda})
    except:
        logger.exception('log error get vivienda response: %s' % response)

def saveVivienda(requestBody):
    try:
        if requestBody['id_iris'] == '1':
            requestBody['species']= 'setosa'
        elif requestBody['id_iris'] == '2':
            requestBody['species']= 'versicolor'
        elif requestBody['id_iris'] == '3':
            requestBody['species']= 'virginica'
        else:
            requestBody['species']= 'no hay flor'
        print(requestBody)
        with open('Spatial_RM.pkl', 'rb') as file:
            requestBody['value']= str(load(file).predict(np.array([[requestBody['N_bathrooms'],requestBody['N_rooms'],requestBody['parking'], requestBody['Lon'], requestBody['Lat']]]).astype(float))[0])
        table.put_item(Item=requestBody)
        body = {
            'Operation': 'SAVE',
            'Message': 'SUCCESS',
            'Item': requestBody
        }
        return buildResponse(200, body)
    except:
        logger.exception('log error post vivienda response: %s' % requestBody)


def modifyVivienda(id_vivienda,updateCol,updateValue):
    try:
        response = table.update_item(
            Key = {
                'id_vivienda':id_vivienda
            },
            UpdateExpression= 'set %s = :value' % updateCol,
            ExpressionAttributeValues={
                ':value': updateValue
        },
            ReturnValues = 'UPDATED_NEW_DONE'
        )
        body = {
            'Operation': 'UPDATE',
            'Message': 'SUCCESS',
            'UpdateAttributes': response
        }
        return buildResponse(200, body)
    except:
        logger.exception('log error get vivienda response: %s' % response)

def deleteVivienda(id_vivienda):
    try:
        response = table.delete_item(
            Key = {
                'id_vivienda': id_vivienda
            },
            ReturnValues = 'DELETE_DONE'
        )
        body = {
            'Operation': 'DELETE',
            'Message': 'SUCCESS',
            'UpdateAttributes': response
        }
        return buildResponse(200, body)
    except:
        logger.exception('log error delete vivienda response: %s' % response)

