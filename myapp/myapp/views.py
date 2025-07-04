from azure_auth.decorators import azure_auth_required
from django.shortcuts import HttpResponse, render
from django.http import JsonResponse
import requests
import json
from azure_auth.handlers import AuthHandler
import msal
from myapp.api.apache_livy import ApacheLivy

from dotenv import load_dotenv
import os
# Load environment variables from .env file
load_dotenv()

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
        livy_expires_in = request.session.get('livy_expires_in', None)    
        livy_session_id = request.session.get('livy_session_id', None) 
        livy_statement_ids = request.session.get('livy_statement_ids', None)      
    else:       
        access_token = None 
        user = None
        expires_in = None
        livy_token = None
        livy_expires_in = None
        livy_session_id = None
        livy_statement_ids = None
        
    return render(request, 'index.html', dict(        
        access_token = access_token,           
        user = user,
        expires_in = expires_in,
        livy_token = livy_token,  
        livy_expires_in = livy_expires_in,
        livy_session_id = livy_session_id,
        livy_statement_ids = livy_statement_ids,
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
        os.getenv('GRAPH_USER_ENDPOINT'),  
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
         os.getenv('GRAPH_MEMBER_ENDPOINT'),
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
def createLivySession(request):      
    try:
        # Get a Livy Token
        if(request.session.get('livy_token')):
             livy_token = request.session.get('livy_token')             
        else:
            accounts = AuthHandler(request).msal_app.get_accounts()
            fabric_result = AuthHandler(request).msal_app.acquire_token_silent(
                    scopes=["https://api.fabric.microsoft.com/.default"], account=accounts[0]
                )
            livy_token = fabric_result['access_token']
            request.session['livy_token'] = livy_token
            request.session['livy_expires_in'] = fabric_result['expires_in']            
        
        # Get a Livy session ID
        if(request.session.get('livy_session_id')):
            livy_session_id = request.session.get('livy_session_id')
            sessionExists = "Already exists, "
        else:            
            sessionExists = ""
            Environment_ID = os.getenv('Environment_ID')
            if(Environment_ID):
                # Important: Run against a specific environment. Session starts slower.
                conf = {"spark.fabric.environmentDetails": json.dumps({"id": Environment_ID })}
            else:
                # Important: Run against the default starter pool for the workspace. Session starts faster.
                conf = {}
            
            # Create a session
            api_result = ApacheLivy(base_url=os.getenv("LIVY_BASE_ENDPOINT"), access_token=livy_token, timeout=int(os.getenv("LIVY_REQUESTS_TIMEOUT"))).create_session(
                data={
                    # Use unique session name, something like: G-Link_ClientName/UserName_DateTime
                    "name": "G-link test pyspark session using Livy",
                    "archives": [],                    
                    "conf": conf,
                    "ttl" : int(os.getenv("LIVY_SESSION_TTL")),  # Set the session TTL (in seconds)
                    "tags": {
                    }
                    }
                )if livy_token else "Not authenticated"
                
            livy_session_id = api_result.json()['id']
            request.session['livy_session_id'] = livy_session_id
                                    
            api_result.raise_for_status()  # Check for HTTP errors
        
        return render(request, 'display.html', {
            "title": "Result of Livy request session",
            "content": sessionExists + "Livy Session ID: " + livy_session_id,
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
        # Check Livy Token and Livy Session ID        
        if(request.session.get('livy_token') and request.session.get('livy_session_id')):
            livy_token = request.session.get('livy_token')
            livy_session_id = request.session.get('livy_session_id')
           
            api_result = ApacheLivy(base_url=os.getenv("LIVY_BASE_ENDPOINT"), access_token=livy_token, timeout=int(os.getenv("LIVY_REQUESTS_TIMEOUT"))).get_session(livy_session_id)
            
            livy_state_session = api_result.json()
            api_result.raise_for_status()  # Check for HTTP errors
            
            return render(request, 'display.html', {
                "title": "Result of Livy check session",
                "content": "Livy Session ID: " + livy_session_id + "\r\nState:" + json.dumps(livy_state_session, indent=4),                       
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
        # Check Livy Token and Livy Session ID        
        if(request.session.get('livy_token') and request.session.get('livy_session_id')):
            livy_token = request.session.get('livy_token')
            livy_session_id = request.session.get('livy_session_id')

            api_result = ApacheLivy(base_url=os.getenv("LIVY_BASE_ENDPOINT"), access_token=livy_token, timeout=int(os.getenv("LIVY_REQUESTS_TIMEOUT"))).submit_statement(livy_session_id, livy_code)
            
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
                    "content": "Livy Session ID: " + livy_session_id + "\r\nStatement ID:" + str(livy_statement_id),                       
                })  
            else:
                 return render(request, 'display.html', {
                    "title": "Result of Livy remote code execution",
                    "content": "Livy Session ID: " + livy_session_id + "\r\nResult:" + str(api_result),                       
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
        # Check Livy Token and Livy Session ID        
        if(request.session.get('livy_token') and request.session.get('livy_session_id')):
            livy_token = request.session.get('livy_token')
            livy_session_id = request.session.get('livy_session_id')

            statement_id = request.GET.get('id', None)
            api_result = ApacheLivy(base_url=os.getenv("LIVY_BASE_ENDPOINT"), access_token=livy_token, timeout=int(os.getenv("LIVY_REQUESTS_TIMEOUT"))).get_statement(livy_session_id,statement_id)
                    
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
                    "content": "Livy Session ID: " + livy_session_id + "\r\nResult:\r\n" + result,                       
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
        # Check Livy Token and Livy Session ID        
        if(request.session.get('livy_token') and request.session.get('livy_session_id')):
            livy_token = request.session.get('livy_token')
            livy_session_id = request.session.get('livy_session_id')
            
            api_result = ApacheLivy(base_url=os.getenv("LIVY_BASE_ENDPOINT"), access_token=livy_token, timeout=int(os.getenv("LIVY_REQUESTS_TIMEOUT"))).delete_session(livy_session_id)
            api_result.raise_for_status()  # Check for HTTP errors
            
            # drop the Livy session id and the statements from the session (if result 200)
            request.session['livy_session_id'] = None
            request.session['livy_statement_ids'] = None
            
            return render(request, 'display.html', {
                "title": "Result of Livy delete session",
                "content": "Livy Session ID: " + livy_session_id + "\r\nAPI result:\r\n" + str(api_result),                       
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
    
    # Clean session
    request.session['livy_session_id'] = None
    request.session['access_token'] = None
    request.session['livy_statement_ids'] = None
    
    return render(request, 'index.html', dict(         
        title = title,        
    ))
