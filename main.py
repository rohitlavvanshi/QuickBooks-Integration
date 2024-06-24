import requests
import json
import base64
import os
from urllib.parse import urlencode
from datetime import datetime, timedelta
import time
from fastapi import FastAPI, HTTPException
import pyodbc
from dotenv import load_dotenv

app = FastAPI()

load_dotenv()

client_id = os.getenv('CLIENT_ID')
client_secret = os.getenv('CLIENT_SECRET')
redirect_uri = os.getenv('REDIRECT_URI')
company_id = os.getenv('COMPANY_ID')


token_url = 'https://oauth.platform.intuit.com/oauth2/v1/tokens/bearer'
auth_url = 'https://appcenter.intuit.com/connect/oauth2'
token_file_path = 'C:/Users/DELL/Desktop/Quickbooks/token.json'
connection_string = 'Driver={ODBC Driver 18 for SQL Server};Server=tcp:thenetreturn.database.windows.net,1433;Database=TNR;Uid=tnr@growwstacks.com@thenetreturn;Pwd=thenetreturn@2024;Encrypt=yes;TrustServerCertificate=no;Connection Timeout=30'

@app.get('/')
async def root():
    return "Server is running..."

def get_authorization_url(client_id, redirect_uri):
    try:
        auth_params = {
            'client_id': client_id,
            'response_type': 'code',
            'scope': 'com.intuit.quickbooks.accounting',
            'redirect_uri': redirect_uri,
            'state': 'random_string_for_csrf_protection'
        }
        url = f"{auth_url}?{urlencode(auth_params)}"
        return url
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to generate authorization URL: {e}")

def get_tokens(auth_code, client_id, client_secret, redirect_uri):
    try:
        auth_string = f"{client_id}:{client_secret}"
        auth_header = base64.b64encode(auth_string.encode()).decode()

        headers = {
            'Content-Type': 'application/x-www-form-urlencoded',
            'Authorization': f'Basic {auth_header}'
        }

        data = {
            'grant_type': 'authorization_code',
            'code': auth_code,
            'redirect_uri': redirect_uri
        }

        response = requests.post(token_url, headers=headers, data=urlencode(data))
        tokens = response.json()

        if 'access_token' in tokens and 'refresh_token' in tokens:
            tokens['expires_at'] = datetime.now().timestamp() + tokens.get('expires_in', 3600)
            return tokens
        else:
            raise HTTPException(status_code=500, detail=f"Failed to get tokens: {tokens}")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to get tokens: {e}")

def get_headers(access_token):
    return {
        'Authorization': f'Bearer {access_token}',
        'Accept': 'application/json',
        'Content-Type': 'application/json'
    }

def refresh_access_token(refresh_token):
    try:
        auth_string = f"{client_id}:{client_secret}"
        auth_header = base64.b64encode(auth_string.encode()).decode()

        headers = {
            'Content-Type': 'application/x-www-form-urlencoded',
            'Authorization': f'Basic {auth_header}'
        }

        data = {
            'grant_type': 'refresh_token',
            'refresh_token': refresh_token
        }

        response = requests.post(token_url, headers=headers, data=urlencode(data))
        tokens = response.json()

        if 'access_token' in tokens and 'refresh_token' in tokens:
            tokens['expires_at'] = datetime.now().timestamp() + tokens.get('expires_in', 3600)
            return tokens
        else:
            raise HTTPException(status_code=500, detail=f"Failed to refresh tokens: {tokens}")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to refresh tokens: {e}")

def load_tokens():
    try:
        with open(token_file_path, 'r') as f:
            tokens = json.load(f)
        return tokens
    except FileNotFoundError:
        return {}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to load tokens: {e}")

def save_tokens(tokens):
    try:
        with open(token_file_path, 'w') as f:
            json.dump(tokens, f)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to save tokens: {e}")

def make_api_call(api_url):
    try:
        tokens = load_tokens()
        retry_attempts = 5
        retry_delay = 1

        if not tokens:
            raise HTTPException(status_code=401, detail="Tokens not found. Please authenticate.")

        if tokens.get('expires_at', 0) <= datetime.now().timestamp():
            tokens = refresh_access_token(tokens.get('refresh_token', ''))
            save_tokens(tokens)

        headers = get_headers(tokens['access_token'])

        for attempt in range(retry_attempts):
            response = requests.get(api_url, headers=headers)

            if response.status_code == 200:
                return response.json()
            elif response.status_code == 401:
                tokens = refresh_access_token(tokens.get('refresh_token', ''))
                save_tokens(tokens)
                headers = get_headers(tokens['access_token'])
            elif response.status_code == 429:
                time.sleep(retry_delay)
                retry_delay *= 2
            else:
                time.sleep(retry_delay)
                retry_delay *= 2

        raise HTTPException(status_code=500, detail="Failed to make API call after multiple attempts")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to make API call: {e}")

def parse_trial_balance(data):
    try:
        accounts = {}
        if 'Rows' in data and 'Row' in data['Rows']:
            for row in data['Rows']['Row']:
                if 'ColData' in row and len(row['ColData']) >= 3:
                    account_name = row['ColData'][0]['value']
                    debit = float(row['ColData'][1]['value']) if row['ColData'][1]['value'] else 0.0
                    credit = float(row['ColData'][2]['value']) if row['ColData'][2]['value'] else 0.0
                    accounts[account_name] = {'debit': debit, 'credit': credit}
        return accounts
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to parse trial balance: {e}")

def subtract_values(prev_data, curr_data):
    try:
        result = {}
        all_keys = set(prev_data.keys()).union(set(curr_data.keys()))
        for key in all_keys:
            prev_values = prev_data.get(key, {'debit': 0, 'credit': 0})
            curr_values = curr_data.get(key, {'debit': 0, 'credit': 0})
            result[key] = {
                'account_name': key,
                'debit_diff': curr_values['debit'] - prev_values['debit'],
                'credit_diff': curr_values['credit'] - prev_values['credit']
            }
        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to subtract values: {e}")

def insert_data_into_db(data, date):
    try:
        conn = pyodbc.connect(connection_string)
        cursor = conn.cursor()

        for entry in data:
            cursor.execute("""
                INSERT INTO test_rohit (Account, Credit, Debit, date)
                VALUES (?, ?, ?, ?)
            """, entry['account_name'], entry['credit_diff'], entry['debit_diff'], date)

        conn.commit()
        cursor.close()
        conn.close()
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Database insertion failed: {e}")

@app.get("/process_fixed_dates")
def process_fixed_dates():
    today_date = datetime.now().strftime('%Y-%m-%d')
    yesterday_date = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
    #today_date = '2024-01-02'
    #yesterday_date = '2024-01-03'

    api_url_today = f'https://quickbooks.api.intuit.com/v3/company/{company_id}/reports/TrialBalance?start_date={today_date}&end_date={today_date}'
    api_url_yesterday = f'https://quickbooks.api.intuit.com/v3/company/{company_id}/reports/TrialBalance?start_date={yesterday_date}&end_date={yesterday_date}'

    try:
        today_data = make_api_call(api_url_today)
        yesterday_data = make_api_call(api_url_yesterday)

        today_accounts = parse_trial_balance(today_data)
        yesterday_accounts = parse_trial_balance(yesterday_data)

        balance_diff = subtract_values(yesterday_accounts, today_accounts)
        
        # Insert today's balance differences into the database with today's date
        insert_data_into_db(list(balance_diff.values()), today_date)

        return {"Balance Differences": list(balance_diff.values())}
    except Exception as ex:
        return(f"Something went wrong \n", ex)
