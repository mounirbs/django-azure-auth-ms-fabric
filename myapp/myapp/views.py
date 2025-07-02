from azure_auth.decorators import azure_auth_required
from django.shortcuts import HttpResponse, render
from django.http import JsonResponse
import requests
import json
from azure_auth.handlers import AuthHandler
import msal

from dotenv import load_dotenv
import os
# Load environment variables from .env file
load_dotenv()

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
        fabric_token = request.session.get('fabric_token', None)
        fabric_expires_in= request.session.get('fabric_expires_in', None)    
        fabric_session_id = request.session.get('fabric_session_id', None) 
        fabric_statement_ids = request.session.get('fabric_statement_ids', None)      
    else:       
        access_token = None 
        user = None
        expires_in = None
        fabric_token = None
        fabric_expires_in = None
        fabric_session_id = None
        fabric_statement_ids = None
        
    return render(request, 'index.html', dict(        
        access_token = access_token,           
        user = user,
        expires_in = expires_in,
        fabric_token = fabric_token,
        fabric_expires_in = fabric_expires_in,
        fabric_session_id = fabric_session_id,
        fabric_statement_ids = fabric_statement_ids,
        title = "Microsoft Fabric-Spark remote execution(Livy). Authentication using Microsoft EntraID on Django(django-azure-auth)",
    ))    


@azure_auth_required
def login(request):
    return render(request, 'index.html', dict(
        access_token=AuthHandler(request).get_token_from_cache()['access_token'],              
        user=AuthHandler(request).claims['name'],    
        title="Microsoft Fabric-Spark remote execution(Livy). Authentication using Microsoft EntraID on Django(django-azure-auth)",        
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
def startLivySession(request):      
    try:
        # Get a Fabric Token
        if(request.session.get('fabric_token')):
             fabric_token = request.session.get('fabric_token')             
        else:
            accounts = AuthHandler(request).msal_app.get_accounts()
            fabric_result = AuthHandler(request).msal_app.acquire_token_silent(
                    scopes=["https://api.fabric.microsoft.com/.default"], account=accounts[0]
                )
            fabric_token = fabric_result['access_token']
            request.session['fabric_token'] = fabric_token
            request.session['fabric_expires_in'] = fabric_result['expires_in']            
        
        # Get a Fabric session ID
        if(request.session.get('fabric_session_id')):
            fabric_session_id = request.session.get('fabric_session_id')
            sessionExists = "Already exists, "
        else:            
            sessionExists = ""
            Environment_ID = os.getenv('Environment_ID')
            if(Environment_ID):
                # Important: Run against the default starter pool for the workspace. Session start is faster.
                conf = {"spark.fabric.environmentDetails": json.dumps({"id": Environment_ID })}
            else:
                # Important: Run against a specific environment. Session start is slower.
                conf = {}
            
            api_result = requests.post(
                os.getenv("LIVY_ENDPOINT"),                
                headers={'Authorization': 'Bearer ' + fabric_token},                             
                json={
                    # Use unique session name, something like: G-Link_ClientName/UserName_DateTime
                    "name": "G-link test pyspark session using Livy",
                    "archives": [],                    
                    "conf": conf,
                    "tags": {
                    },                  
                },
                timeout=30,
            ) if fabric_token else "Not authenticated"
            
            fabric_session_id = api_result.json()['id']
            request.session['fabric_session_id'] = fabric_session_id
                                    
            api_result.raise_for_status()  # Check for HTTP errors
        
        return render(request, 'display.html', {
            "title": "Result of Fabric Livy request session",
            "content": sessionExists + "Fabric Session ID: " + fabric_session_id,
            "fabric_session_id": fabric_session_id           
        })        
    except requests.exceptions.RequestException as e:
        return render(request, 'display.html', {
            "title": "Result of Fabric Livy request session",
            "content": JsonResponse({'status': 'error', 'message': str(e)}).content.decode('utf-8')
        })
        
@azure_auth_required
def checkLivySession(request):      
    try:
        # Check Fabric Token and Fabric Session ID        
        if(request.session.get('fabric_token') and request.session.get('fabric_session_id')):
            fabric_token = request.session.get('fabric_token')
            fabric_session_id = request.session.get('fabric_session_id')

            api_result = requests.get(
                os.getenv("LIVY_ENDPOINT") + "/" + fabric_session_id,                
                headers={'Authorization': 'Bearer ' + fabric_token},                
                timeout=30,
            )            
            fabric_state_session = api_result.json()
            api_result.raise_for_status()  # Check for HTTP errors
            
            return render(request, 'display.html', {
                "title": "Result of Fabric Livy check session",
                "content": "Fabric Session ID: " + fabric_session_id + "\r\nState:" + fabric_state_session["state"],                       
            })   
        else:
            return render(request, 'display.html', {
                "title": "Result of Fabric Livy check session",
                "content": "No Fabric Token and/or Fabric session ID. Please Start Livy Session first"             
            })    
                  
    except requests.exceptions.RequestException as e:
        return render(request, 'display.html', {
            "title": "Result of Fabric Livy check session",
            "content": JsonResponse({'status': 'error', 'message': str(e)}).content.decode('utf-8')
        })
    
@azure_auth_required
def submitFabricCode(request):
    fabric_code = request.POST.get('fabric_livy_code', None)
    try:
        # Check Fabric Token and Fabric Session ID        
        if(request.session.get('fabric_token') and request.session.get('fabric_session_id')):
            fabric_token = request.session.get('fabric_token')
            fabric_session_id = request.session.get('fabric_session_id')

            api_result = requests.post(
                os.getenv("LIVY_ENDPOINT") + "/" + fabric_session_id + "/statements",                
                headers={'Authorization': 'Bearer ' + fabric_token},
                json={
                    "code": f"{fabric_code}",
                    "kind": "pyspark"
                },
                timeout=30,
            )
            
            if('id' in api_result.json()):
                fabric_statement_id = api_result.json()['id']
                
                #store statementIds in a session
                if(request.session.get('fabric_statement_ids')):
                    ids = request.session.get('fabric_statement_ids', [])    
                else:
                    ids = []
                                    
                ids.append(fabric_statement_id)
                request.session['fabric_statement_ids'] = ids
                
                return render(request, 'display.html', {
                    "title": "Result of Fabric Livy remote code execution",
                    "content": "Fabric Session ID: " + fabric_session_id + "\r\nStatement ID:" + str(fabric_statement_id),                       
                })  
            else:
                 return render(request, 'display.html', {
                    "title": "Result of Fabric Livy remote code execution",
                    "content": "Fabric Session ID: " + fabric_session_id + "\r\nResult:" + str(api_result),                       
                }) 
        else:
            return render(request, 'display.html', {
                "title": "Result of Fabric Livy remote code execution",
                "content": "No Fabric Token and/or Fabric session ID. Please Start Livy Session first"             
            })          
        
    except requests.exceptions.RequestException as e:
        return render(request, 'display.html', {
            "title": "Result of Fabric Livy remote code execution",
            "content": JsonResponse({'status': 'error', 'message': str(e)}).content.decode('utf-8')
        })    

@azure_auth_required
def getStatement(request):      
    try:
        # Check Fabric Token and Fabric Session ID        
        if(request.session.get('fabric_token') and request.session.get('fabric_session_id')):
            fabric_token = request.session.get('fabric_token')
            fabric_session_id = request.session.get('fabric_session_id')

            statement_id = request.GET.get('id', None)
            
            api_result = requests.get(
                os.getenv("LIVY_ENDPOINT") + "/" + fabric_session_id + "/statements/" + statement_id,                
                headers={'Authorization': 'Bearer ' + fabric_token},                
                timeout=30,
            )            
            fabric_statement = api_result.json()
            api_result.raise_for_status()  # Check for HTTP errors
            
            if(fabric_statement["state"] != "available"):            
               result =  json.dumps(fabric_statement, indent=4)
            else: 
                try:
                    result = fabric_statement['output']['data']['text/plain']
                except:
                    result = json.dumps(fabric_statement, indent=4)                
            
            return render(request, 'display.html', {
                    "title": "Result of Fabric Statement:" + statement_id,
                    "content": "Fabric Session ID: " + fabric_session_id + "\r\nResult:\r\n" + result,                       
                })    
        else:
            return render(request, 'display.html', {
                "title": "Result of Fabric Statement:" + statement_id,
                "content": "No Fabric Token and/or Fabric session ID. Please Start Livy Session first"             
            })    
                  
    except requests.exceptions.RequestException as e:
        return render(request, 'display.html', {
            "title": "Result of Fabric Livy check session",
            "content": JsonResponse({'status': 'error', 'message': str(e)}).content.decode('utf-8')
        })
        
@azure_auth_required
def stopLivySession(request):      
    try:
        # Check Fabric Token and Fabric Session ID        
        if(request.session.get('fabric_token') and request.session.get('fabric_session_id')):
            fabric_token = request.session.get('fabric_token')
            fabric_session_id = request.session.get('fabric_session_id')

            api_result = requests.delete(
                os.getenv("LIVY_ENDPOINT") + "/" + fabric_session_id,                
                headers={'Authorization': 'Bearer ' + fabric_token},                
                timeout=30,
            )
            api_result.raise_for_status()  # Check for HTTP errors
            
            # drop the fabric session id and the statements from the session (if result 200)
            request.session['fabric_session_id'] = None
            request.session['fabric_statement_ids'] = None
            
            return render(request, 'display.html', {
                "title": "Result of Fabric Livy delete session",
                "content": "Fabric Session ID: " + fabric_session_id + "\r\nAPI result:\r\n" + str(api_result),                       
            })   
        else:
            return render(request, 'display.html', {
                "title": "Result of Fabric Livy delete session",
                "content": "No Fabric Token and/or Fabric session ID. Please Start Livy Session first"             
            })    
                  
    except requests.exceptions.RequestException as e:
        return render(request, 'display.html', {
            "title": "Result of Fabric Livy delete session",
            "content": JsonResponse({'status': 'error', 'message': str(e)}).content.decode('utf-8')
        })
      
@azure_auth_required
def logout(request):    
    # TODO Stop the Fabric Session - stopLivySession
    
    # Clean session
    request.session['fabric_session_id'] = None
    request.session['fabric_token'] = None
    request.session['fabric_statement_ids'] = None
    
    return render(request, 'index.html', dict(         
        title="Microsoft Fabric-Spark remote execution(Livy). Authentication using Microsoft EntraID on Django(django-azure-auth)",        
    ))
