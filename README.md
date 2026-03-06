# adm-api-request
Production worker that periodically sends synthetic requests to the ADM API (POST /predict), collects model outputs (score, confidence, agreement), and stores them in PostgreSQL for monitoring and evaluation.
