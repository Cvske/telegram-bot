import gspread
from pprint import pprint
from datetime import datetime
from oauth2client.service_account import ServiceAccountCredentials
import yaml
from apiclient import discovery
from httplib2 import Http
from oauth2client import client, file, tools
import json
import boto3
from boto3.dynamodb.conditions import Key, Attr
import requests

TELE_TOKEN = '**********************'
URL = "https://api.telegram.org/bot{}/".format(TELE_TOKEN)

# google docs
SCOPES = 'https://www.googleapis.com/auth/documents.readonly'
# google sheets
scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
DISCOVERY_DOC = 'https://docs.googleapis.com/$discovery/rest?version=v1'


creds = ServiceAccountCredentials.from_json_keyfile_name('client_secret2.json', scope)
client = gspread.authorize(creds)



# google docs ID
DOCUMENT_ID = '****************************'


def get_session_id(user_id):
    dynamodb = boto3.resource("dynamodb")
    table = dynamodb.Table("hours_reports")
    response = table.query(KeyConditionExpression = Key("user_id").eq(user_id) & Key('session_id').gt(0))
    if len(response['Items']) == 0:
        session_id = 0
    else: 
        session_id = int(response['Items'][len(response['Items'])-1]['session_id'])
    
    return session_id


def get_file(url):
    file_path = '/tmp/model.yaml'  # path where we save the file
    # url to get data is different than others
    response = requests.get(url)  # get file by url

    with open(file_path, 'wb') as f:  # open file and store in file_path
        f.write(response.content)

    with open(file_path) as f:  # open file and read as yaml file
        data = yaml.load(f, Loader=yaml.FullLoader)  # create yaml object

    return data



def send_message(text, chat_id):
    url = URL + "sendMessage?text={}&chat_id={}".format(text, chat_id)
    requests.get(url)


def lambda_handler(event, context):
    #open DynamoDB
    #return {'statusCode': 200}
    dynamodb = boto3.resource("dynamodb")
    table = dynamodb.Table("hours_reports")

    url = requests.get(
        'https://script.google.com/macros/s/********************************').content

    data = json.loads(event['body'])  # 1. get data from request
    message = data['message']
    chat_id = message['chat']['id']  # line 7-10 set important data from the message to variables
    user_id = int(message['from']['id'])
    user_name = message['from']['first_name'] + " " + message['from']['last_name']
    
    
    
   
    
    try:  # check is text in message
        reply = message["text"]
    except:
        return {'statusCode': 200}


    
        
        

    if (reply == '/start'):
        session_id = get_session_id(user_id)
    
        
        
        
        raport_name_array = []
        data = get_file(url)
        send_message('Wybierz raport: ', chat_id)
        
        for i in data['Reports']:
            raport_name_array.append(i)
            send_message(i, chat_id)
            

        item = {
        'user_id': user_id,
        'session_id': session_id + 1,
        'answers': [],
        'counter': 0,
        'questions': [],
        'report_name': '',
        'state': 'Report', 
        }
        response = table.put_item(Item=item)

   
    
    session_id = get_session_id(user_id)    
    response = table.get_item(Key={'user_id': user_id, 'session_id': session_id})
    state = response['Item']['state']    
    
    
        
    if(state == 'Report' and reply != '/start'):
        
        
        raport_name_array = []
        data = get_file(url)
        reports = data['Reports']
        
        if reply not in reports:
            send_message('Podano nieistniejącą nazwę reportu! Podaj poprawną: ', chat_id)
            return {'statusCode': 200}
              
        try:
            response = table.get_item(Key={'user_id': user_id, 'session_id': session_id})
        except ClientError as e:
            return {'statusCode': 200}
        else:
            arr = []
            arr = response['Item']['questions']
            counter = int(response['Item']['counter'])
            if(counter != 0):
                question = arr[counter]
            max_length = int(len(arr))
        
        
        response = table.update_item(Key={'user_id': user_id, 'session_id': session_id},
                                     UpdateExpression="SET #s = :r",
                                     ExpressionAttributeNames={
                                         '#s': "report_name"
                                     },
                                     ExpressionAttributeValues={
                                         ':r': reply,
                                     })
        
        
        if (counter == 0):
            questions = []
            data = get_file(url)
            spreadsheet = data['Reports'][reply]['spreadsheet_id']
            for i in data['Reports'][reply]['questions']:
                questions.append(i)

                                         
            response = table.update_item(Key={'user_id': user_id, 'session_id': session_id},
                                         UpdateExpression="SET #s = :r, #q = :n",
                                         ExpressionAttributeNames={
                                             '#s': "state",
                                             '#q': "questions"
                                         },
                                         ExpressionAttributeValues={
                                             ':r': 'Answers',
                                             ':n': questions
                                         })                             


        try:
            response = table.get_item(Key={'user_id': user_id, 'session_id': session_id})
        except ClientError as e:
            return {'statusCode': 200}
        else:
            arr = []
            arr = response['Item']['questions']
            question = arr[counter]

        send_message(question, chat_id)
        
        
        
    elif(state == 'Answers' and reply != '/start'):
        
        try:
            response = table.get_item(Key={'user_id': user_id, 'session_id': session_id})
        except ClientError as e:
            return {'statusCode': 200}
        else:
            arr = []
            arr = response['Item']['questions']
            counter = int(response['Item']['counter'])
            question = arr[counter]
            max_length = int(len(arr))
        
        
        
        
        
        #add answers to database
        response = table.update_item(Key={'user_id': user_id, 'session_id': session_id},
                                        UpdateExpression="SET answers = list_append(answers, :i)",
                                            ExpressionAttributeValues={
                                                ':i': [reply],
                                            },
                                            ReturnValues="UPDATED_NEW"
                                        )
            
        
        
        
            
            
        if ((max_length - 1) == (counter)):
            send_message('Zakonczono dodawanie! Jesli chcesz dodac nowe dane, uzyj opcji "/start" ', chat_id)
            
            try:
                response = table.get_item(Key={'user_id': user_id, 'session_id': session_id})
            except ClientError as e:
                return {'statusCode': 200}
            else:
                data = get_file(url)
                report = response['Item']['report_name']
                spreadsheet = data['Reports'][report]['spreadsheet_id']
                #send_message(spreadsheet, chat_id)
                
                currentDate = datetime.today().strftime('%Y-%m-%d %H:%M:%S')
                answers = []
                answers = response['Item']['answers']
                
                sheet = client.open(spreadsheet).sheet1
                lista = sheet.get_all_records()
                
                #print(lista)
            
                if not lista:
                    liczbaWierszy = 0
                    liczbaKolumn = 0
                    wiersz = 2
                else:
                    liczbaWierszy = len(lista)
                    liczbaKolumn = len(lista[0])
                    wiersz = liczbaWierszy + 3

                # insert
            
                
                for i in answers:
                    x = i
                    sheet.update_cell(wiersz, 1, x)  #wiersze, kolumny
                    wiersz = wiersz + 1
                    
                if not lista:
                    sheet.update_cell(liczbaWierszy + 1, 1, currentDate)
                else:
                    sheet.update_cell(liczbaWierszy + 2, 1, currentDate)
                
            
            
            
            
            
            
            response = table.update_item(Key={'user_id': user_id, 'session_id': session_id},
                                         UpdateExpression="SET #s = :r",
                                         ExpressionAttributeNames={
                                             '#s': "state"
                                         },
                                         ExpressionAttributeValues={
                                             ':r': 'Done',
                                         })


        else:
            counter = counter + 1
            response = table.update_item(Key={'user_id': user_id, 'session_id': session_id},
                                         UpdateExpression="SET #s = :r",
                                         ExpressionAttributeNames={
                                             '#s': "counter"
                                         },
                                         ExpressionAttributeValues={
                                             ':r': counter,
                                         })
        
            try:
                response = table.get_item(Key={'user_id': user_id, 'session_id': session_id})
            except ClientError as e:
                return {'statusCode': 200}
            else:
                arr = []
                arr = response['Item']['questions']
                counter = int(response['Item']['counter'])
                question = arr[counter]
                
                
                send_message(question, chat_id)


    return {
        'statusCode': 200
    }