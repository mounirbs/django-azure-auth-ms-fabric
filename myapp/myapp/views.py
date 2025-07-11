import time
from azure_auth.decorators import azure_auth_required
from django.shortcuts import HttpResponse, render
from django.http import JsonResponse
import requests
import json
from datetime import datetime, timedelta
from azure_auth.handlers import AuthHandler
import msal
from tomlkit import date
from myapp.api.apache_livy import ApacheLivy

from dotenv import load_dotenv
import os
# Load environment variables from .env file
load_dotenv()
graph_user_endpoint = os.getenv('GRAPH_USER_ENDPOINT')
graph_member_endpoint = os.getenv('GRAPH_MEMBER_ENDPOINT')
livy_base_url = os.getenv("LIVY_BASE_ENDPOINT")
livy_requests_timeout = int(os.getenv("LIVY_REQUESTS_TIMEOUT"))
livy_session_name_prefix = os.getenv("LIVY_SESSION_NAME_PREFIX")
livy_spark_conf = os.getenv('LIVY_SPARK_CONF') if os.getenv('LIVY_SPARK_CONF') else "{}"
livy_backend = os.getenv("LIVY_BACKEND").strip().lower()
livy_backend_spark_dependencies = os.getenv("LIVY_SPARK_DEPENDENCIES") if os.getenv("LIVY_SPARK_DEPENDENCIES") else ""

title = "Apache Livy/Microsoft Fabric - Spark remote execution. Authentication using Microsoft EntraID with django-azure-auth"

def user_mapping_fn(**attributes):
    #https://docs.djangoproject.com/en/5.2/ref/contrib/auth/#django.contrib.auth.models.User
    return {
        "first_name": attributes["givenName"],
        "last_name": attributes["surname"],
        "email": attributes["upn"],
        "is_staff": True,        
    }

def index(request):     
    if(AuthHandler(request).get_token_from_cache()):      
        access_token = AuthHandler(request).get_token_from_cache()['access_token']        
        expires_in = AuthHandler(request).get_token_from_cache()['expires_in']     
        user = AuthHandler(request).claims['name']
        
        livy_token = request.session.get('livy_token', None)
        livy_token_expiration_time = request.session.get('livy_token_expiration_time', None)
        livy_session_id = request.session.get('livy_session_id', None) 
        livy_statement_ids = request.session.get('livy_statement_ids', None)  
        
    else:       
        access_token = None 
        user = None
        expires_in = None
        livy_token = None
        livy_token_expiration_time = None
        livy_session_id = None
        livy_statement_ids = None
        
    return render(request, 'index.html', dict(        
        access_token = access_token,           
        user = user,
        expires_in = expires_in,
        livy_token = livy_token,  
        livy_expires_in = str(int((datetime.strptime(livy_token_expiration_time, "%Y-%m-%d %H:%M:%S") - datetime.now()).total_seconds())) if livy_token_expiration_time else None,
        livy_session_id = livy_session_id,
        livy_statement_ids = livy_statement_ids,
        livy_backend = livy_backend.upper(),
        title = title,
    ))    


@azure_auth_required
def login(request):
    return render(request, 'index.html', dict(
        access_token = AuthHandler(request).get_token_from_cache()['access_token'],              
        user = AuthHandler(request).claims['name'],    
        title = title,        
    ))


@azure_auth_required
def me(request):
    api_result = requests.get(  # Use access token to call a web api
        graph_user_endpoint,  
        headers={'Authorization': 'Bearer ' + AuthHandler(request).get_token_from_cache()['access_token']},
        timeout=30,
    ).json() if AuthHandler(request).user_is_authenticated else "Not authenticated"
    return render(request, 'display.html', {
        "title": "Result of Me",
        "content": json.dumps(api_result, indent=4)
    })
    
@azure_auth_required
def memberOf(request):
    api_result = requests.get(  # Use access token to call a web api
        graph_member_endpoint,
        headers={'Authorization': 'Bearer ' + AuthHandler(request).get_token_from_cache()['access_token']},
        timeout=30,
    ).json() if AuthHandler(request).user_is_authenticated else "Not authenticated"
    
    # Get the memberOf groups
    memberOf = [group["displayName"] for group in api_result["value"] if "displayName" in group]
        
    return render(request, 'display.html', {
        "title": "Result of Member Of",
        "content": json.dumps(memberOf, indent=4)
    }) 


@azure_auth_required
def requestLivyFabricToken(request):

    livy_token = getLivyToken(request)
    return render(request, 'display.html', {
            "title": "Result of Livy/Fabric request token",
            "content": "Livy/Fabric Token: " + livy_token     
        }) 
    
@azure_auth_required
def createLivySession(request):      
    try:
        # Get a Livy session ID
        if(request.session.get('livy_session_id')):
            livy_session_id = request.session.get('livy_session_id')
            sessionExists = "Already exists, "
        else:            
            sessionExists = ""
                        
            # Create a session
            livy_token = getLivyToken(request)
            livy = livyGetOrCreate(livy_token)
             
            api_result = livy.create_session(
                data={
                    # Ideally, use unique session name
                    "name": livy_session_name_prefix + datetime.now().strftime("%Y-%m-%d %H:%M:%S"), 
                    "kind": "pyspark",
                    "archives": [],
                    "pyFiles": livy_backend_spark_dependencies.split(',') if livy_backend_spark_dependencies else [],
                    "conf": json.loads(livy_spark_conf) if livy_spark_conf else {},
                    #"idleTimeout" : "10m", # Not working 
                    #"ttl": "10m", # Not working 
                    }
                )if livy_token else "Not authenticated"
           
            api_result.raise_for_status()  # Check for HTTP errors
           
            if(api_result.json()['id']):                
                livy_session_id = api_result.json()['id']
                request.session['livy_session_id'] = livy_session_id
            else:
                return render(request, 'display.html', {
                    "title": "Result of Livy request session",
                    "content": "Livy Session ID: " +  "\r\nResult:" + str(api_result),                       
                })
        
        return render(request, 'display.html', {
            "title": "Result of Livy request session",
            "content": sessionExists + "Livy Session ID: " + str(livy_session_id),
            "livy_session_id": livy_session_id           
        })        
    except requests.exceptions.RequestException as e:
        return render(request, 'display.html', {
            "title": "Result of Livy request session",
            "content": JsonResponse({'status': 'error', 'message': str(e)}).content.decode('utf-8')
        })
        
@azure_auth_required
def checkLivySession(request):      
    try:
        # Check Livy Session ID        
        if(request.session.get('livy_session_id')):            
            livy_session_id = request.session.get('livy_session_id')
            
            livy_token = getLivyToken(request) 
            livy = livyGetOrCreate(livy_token)           
            api_result = livy.get_session(livy_session_id)
            
            livy_state_session = api_result.json()
            api_result.raise_for_status()  # Check for HTTP errors
            
            return render(request, 'display.html', {
                "title": "Result of Livy check session",
                "content": "Livy Session ID: " + str(livy_session_id) + "\r\nState:" + json.dumps(livy_state_session, indent=4),                       
            })   
        else:
            return render(request, 'display.html', {
                "title": "Result of Livy check session",
                "content": "No Livy Token and/or Livy session ID. Please Start Livy Session first"             
            })    
                  
    except requests.exceptions.RequestException as e:
        return render(request, 'display.html', {
            "title": "Result of Livy check session",
            "content": JsonResponse({'status': 'error', 'message': str(e)}).content.decode('utf-8')
        })
    
@azure_auth_required
def submitLivyStatement(request):
    livy_code = request.POST.get('livy_code', None)
    try:
        # Check Lvy Session ID        
        if(request.session.get('livy_session_id')):
            livy_session_id = request.session.get('livy_session_id')
            
            livy_token = getLivyToken(request) 
            livy = livyGetOrCreate(livy_token)
            api_result = livy.submit_statement(livy_session_id, livy_code)
            
            if('id' in api_result.json()):
                livy_statement_id = api_result.json()['id']
                
                #store statementIds in a session
                if(request.session.get('livy_statement_ids')):
                    ids = request.session.get('livy_statement_ids', [])    
                else:
                    ids = []
                                    
                ids.append(livy_statement_id)
                request.session['livy_statement_ids'] = ids
                
                return render(request, 'display.html', {
                    "title": "Result of Livy remote code execution",
                    "content": "Livy Session ID: " + str(livy_session_id) + "\r\nStatement ID:" + str(livy_statement_id),                       
                })  
            else:
                 return render(request, 'display.html', {
                    "title": "Result of Livy remote code execution",
                    "content": "Livy Session ID: " + str(livy_session_id) + "\r\nResult:" + str(api_result),                       
                }) 
        else:
            return render(request, 'display.html', {
                "title": "Result of Livy remote code execution",
                "content": "No Livy Token and/or Livy session ID. Please Start Livy Session first"             
            })          
        
    except requests.exceptions.RequestException as e:
        return render(request, 'display.html', {
            "title": "Result of Livy remote code execution",
            "content": JsonResponse({'status': 'error', 'message': str(e)}).content.decode('utf-8')
        })    

@azure_auth_required
def getLivyStatement(request):      
    try:
        # Check Livy Session ID        
        if(request.session.get('livy_session_id')):
            livy_session_id = request.session.get('livy_session_id')

            statement_id = request.GET.get('id', None)
            
            livy_token = getLivyToken(request) 
            livy = livyGetOrCreate(livy_token)
            api_result = livy.get_statement(livy_session_id,statement_id)
                    
            livy_statement = api_result.json()
            api_result.raise_for_status()  # Check for HTTP errors
            
            if(livy_statement["state"] != "available"):            
               result =  json.dumps(livy_statement, indent=4)
            else: 
                try:
                    result = livy_statement['output']['data']['text/plain']
                except:
                    result = json.dumps(livy_statement, indent=4)                
            
            return render(request, 'display.html', {
                    "title": "Result of Livy Statement:" + statement_id,
                    "content": "Livy Session ID: " + str(livy_session_id) + "\r\nResult:\r\n" + result,                       
                })    
        else:
            return render(request, 'display.html', {
                "title": "Result of Livy Statement:" + statement_id,
                "content": "No Livy Token and/or Livy session ID. Please Start Livy Session first"             
            })    
                  
    except requests.exceptions.RequestException as e:
        return render(request, 'display.html', {
            "title": "Result of Livy check session",
            "content": JsonResponse({'status': 'error', 'message': str(e)}).content.decode('utf-8')
        })
        
@azure_auth_required
def stopLivySession(request):      
    try:
        # Check Livy Session ID        
        if(request.session.get('livy_session_id')):
            livy_session_id = request.session.get('livy_session_id')
            
            livy_token = getLivyToken(request) 
            livy = livyGetOrCreate(livy_token)
            api_result = livy.delete_session(livy_session_id)
            api_result.raise_for_status()  # Check for HTTP errors
            
            # Clean session  (if result 200)
            cleanLivySession(request)
            
            return render(request, 'display.html', {
                "title": "Result of Livy delete session",
                "content": "Livy Session ID: " + str(livy_session_id) + "\r\nAPI result:\r\n" + str(api_result),                       
            })   
        else:
            return render(request, 'display.html', {
                "title": "Result of Livy delete session",
                "content": "No Livy Token and/or Livy session ID. Please Start Livy Session first"             
            })    
                  
    except requests.exceptions.RequestException as e:
        return render(request, 'display.html', {
            "title": "Result of Livy delete session",
            "content": JsonResponse({'status': 'error', 'message': str(e)}).content.decode('utf-8')
        })
      
@azure_auth_required
def logout(request):    
    # Stop the Livy Session - stopLivySession
    stopLivySession(request)
    
    # Clean Livy Token
    cleanLivyToken(request)
    
    # Clean the Graph token
    request.session['access_token'] = None    
    
    return render(request, 'index.html', dict(         
        title = title,        
    ))

def getLivyToken(request):
    try:
        # Get a Livy Token
        if(request.session.get('livy_token') and 
           request.session.get('livy_token_expiration_time') and 
           int((datetime.strptime(request.session.get('livy_token_expiration_time'), "%Y-%m-%d %H:%M:%S") - datetime.now()).total_seconds()) > 0):
            
            # Token is still valid
            livy_token = request.session.get('livy_token')             
        else: # Request new token
            if livy_backend == "apache":
                livy_token = "dummy_token"  # For Local Apache Livy, we can use a dummy token
                token_expires_in = 9999
            else: # Fabric livy_backend
                # TODO: Handle the case when the livy_backend is not in ("apache", "fabric")
                accounts = AuthHandler(request).msal_app.get_accounts()
                fabric_result = AuthHandler(request).msal_app.acquire_token_silent(
                        scopes=["https://api.fabric.microsoft.com/.default"], account=accounts[0]
                    )
                livy_token = fabric_result['access_token']
                token_expires_in = fabric_result['expires_in']
            
            request.session['livy_token'] = livy_token
            request.session['livy_token_expiration_time'] = (datetime.now() + timedelta(seconds=int(token_expires_in))).strftime("%Y-%m-%d %H:%M:%S")
            
            # delete the livy global variable to reinitialize it with the new token
            if 'livy' in globals():
                del globals()['livy']
                      
        return livy_token
    except requests.exceptions.RequestException as e:
        # production - use logs
        print("Error getting Livy/Fabric token:", str(e))
        return None
            
def cleanLivySession(request):
    request.session['livy_session_id'] = None  
    request.session['livy_statement_ids'] = None
    
def cleanLivyToken(request):
    request.session['livy_token'] = None
    request.session['livy_token_expiration_time'] = None
    
def livyGetOrCreate(access_token):
    global livy
    # Check if livy is already initialized
    if 'livy' in globals():
        return livy
    else:
        # Initialize livy with the provided parameters
        livy = ApacheLivy(base_url=livy_base_url, access_token=access_token, timeout=int(livy_requests_timeout))
        return livy
