import requests
import numpy as np
import psycopg2
import time
import threading
from datetime import datetime
from dotenv import load_dotenv
import os
from fastapi import FastAPI 
import warnings; warnings.filterwarnings('ignore')

load_dotenv()

API_URL = os.getenv("API_URL")
DATABASE_URL = os.getenv("DATABASE_URL")

app = FastAPI()

def getConnection():
    return psycopg2.connect(DATABASE_URL)

conn = getConnection()
cursor = conn.cursor()

def SendDataDB(score, confidence, agreement):
    global conn, cursor
    try:
        cursor.execute(
            """
            INSERT INTO model_executions
            (timestamp, model_score, model_confidence, model_agreement)
            VALUES (%s, %s, %s, %s)
            """,
            (datetime.now().replace(microsecond=0), score, confidence, agreement)
        )
        conn.commit()
    except psycopg2.Error as e:
        print("Database Error:", e)
        conn.rollback()
        conn = getConnection()
        cursor = conn.cursor()

def MakeRequest():
    data = {
        "devices_count": int(np.random.randint(1, 5)),
        "avg_session_duration": float(round(np.random.uniform(-38.0, 2183.0), 2)),
        "api_rate": float(round(np.random.uniform(13.0, 160.0), 2)),
        "geo_jump_km": float(np.random.randint(0, 15300)),
        "activations_24h": int(np.random.randint(0, 9)),
        "failed_login_ratio": float(round(np.random.uniform(0, 1.0), 2)),
        "api_std_7d": float(round(np.random.uniform(0, 72.0), 2)),
        "session_trend": float(round(np.random.uniform(-1205.0, 1241.0), 2))
    }
    try:
        response = requests.post(API_URL, json=data, timeout=5)
        if response.status_code == 200:
            result = response.json()
            score = result.get("Score")
            confidence = result.get("Model_Confidence")
            agreement = result.get("Model_Agreement")
            #SendDataDB(score, confidence, agreement)
            print(f"[{datetime.now().replace(microsecond=0)}] API Request Success : {response.status_code} OK") 
        else:
            print(f"[{datetime.now().replace(microsecond=0)}] API Error: {response.status_code}")
    except requests.exceptions.RequestException as e:
        print("Request Failed:", e)

def Worker():
    while True:
        MakeRequest()
        time.sleep(30)

@app.on_event("startup") 
def start_worker():
    thread = threading.Thread(target=Worker)
    thread.daemon = True
    thread.start()

@app.get("/")
def health():
    return {"status": "Worker running"} 

