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
conn = None
cursor = None
app = FastAPI()

def getConnection():
    return psycopg2.connect(DATABASE_URL)

def SendDataDB(data, score, confidence, agreement,traffic):
    global conn, cursor
    try:
        cursor.execute(
            """
            INSERT INTO model_prediction (
                timestamp,
                devices_count,
                avg_session_duration,
                api_rate,
                geo_jump_km,
                activations_24h,
                failed_login_ratio,
                api_std_7d,
                session_trend,
                model_score,
                confidence,
                agreement,
                traffic
            )
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            """,
            (
                datetime.now().replace(microsecond=0),
                data["devices_count"],
                data["avg_session_duration"],
                data["api_rate"],
                data["geo_jump_km"],
                data["activations_24h"],
                data["failed_login_ratio"],
                data["api_std_7d"],
                data["session_trend"],
                score,
                confidence,
                agreement,
                traffic
            )
        )
        conn.commit()
    except psycopg2.Error as e:
        print("Database Error:", e)
        conn.rollback()
        conn = getConnection()
        cursor = conn.cursor()

def generate_normal():
    return {
        "devices_count": int(np.random.randint(1, 3)),
        "avg_session_duration": float(round(np.random.uniform(200, 900), 2)),
        "api_rate": float(round(np.random.uniform(20, 80), 2)),
        "geo_jump_km": float(np.random.randint(0, 50)),
        "activations_24h": int(np.random.randint(1, 4)),
        "failed_login_ratio": float(round(np.random.uniform(0.0, 0.2), 2)),
        "api_std_7d": float(round(np.random.uniform(5, 25), 2)),
        "session_trend": float(round(np.random.uniform(-100, 100), 2))
    }

def generate_suspicious():
    return {
        "devices_count": int(np.random.randint(2, 4)),
        "avg_session_duration": float(round(np.random.uniform(900, 1500), 2)),
        "api_rate": float(round(np.random.uniform(80, 120), 2)),
        "geo_jump_km": float(np.random.randint(50, 2000)),
        "activations_24h": int(np.random.randint(3, 7)),
        "failed_login_ratio": float(round(np.random.uniform(0.2, 0.6), 2)),
        "api_std_7d": float(round(np.random.uniform(25, 50), 2)),
        "session_trend": float(round(np.random.uniform(-400, 400), 2))
    }

def generate_anomaly():
    return {
        "devices_count": int(np.random.randint(3, 6)),
        "avg_session_duration": float(round(np.random.uniform(1500, 2200), 2)),
        "api_rate": float(round(np.random.uniform(120, 200), 2)),
        "geo_jump_km": float(np.random.randint(2000, 15000)),
        "activations_24h": int(np.random.randint(6, 12)),
        "failed_login_ratio": float(round(np.random.uniform(0.6, 1.0), 2)),
        "api_std_7d": float(round(np.random.uniform(50, 80), 2)),
        "session_trend": float(round(np.random.uniform(-1200, 1200), 2))
    }

def MakeRequest():
    r = np.random.rand()
    if r < 0.7:
        data = generate_normal()
        traffic = "Normal"
    elif r < 0.9:
        data = generate_suspicious()
        traffic = "Suspicious"
    else:
        data = generate_anomaly()
        traffic = "Anomaly"
    try:
        response = requests.post(API_URL, json=data, timeout=5)
        if response.status_code == 200:
            result = response.json()
            score = result.get("Score")
            confidence = result.get("Model_Confidence")
            agreement = result.get("Model_Agreement")
            SendDataDB(data, score, confidence, agreement,traffic)
            print(f"[{datetime.now().replace(microsecond=0)}] API Request Success: {response.status_code} OK")
        else:
            print(f"[{datetime.now().replace(microsecond=0)}] API Error: {response.status_code}")
    except requests.exceptions.RequestException:
        print(f"[{datetime.now().replace(microsecond=0)}] API Connection Failed")

def Worker():
    while True:
        MakeRequest()
        time.sleep(30)

@app.on_event("startup")
def startup_event():
    global conn, cursor
    conn = getConnection()
    cursor = conn.cursor()
    print(f"[{datetime.now().replace(microsecond=0)}] Database connected successfully")
    thread = threading.Thread(target=Worker)
    print(f"[{datetime.now().replace(microsecond=0)}] API Worker Starting")
    thread.daemon = True
    thread.start()

@app.get("/health")
def health():
    return {"status": "Worker running"}