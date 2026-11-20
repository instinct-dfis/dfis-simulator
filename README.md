# Distributed Financial Intelligence Simulator (DFIS)

Single-container deployment: FastAPI backend serves the built React frontend on port 7860 (SQLite fallback, no external DB needed).

Run locally: `docker build -t dfis . && docker run -p 7860:7860 dfis`
