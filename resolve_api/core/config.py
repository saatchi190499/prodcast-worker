"""Project config class"""

import os
from pydantic_settings import BaseSettings
from pathlib import Path
from dotenv import load_dotenv

env_path = Path(__file__).resolve().parents[2] / ".env"
load_dotenv(dotenv_path=env_path)
# TODO: Switch DB_NAME and DB_SERVER
class Settings(BaseSettings):
    PROJECT_NAME: str = os.getenv('PROJECT_NAME', 'Results Gathering From Resolve')
    VERSION: str = os.getenv('VERSION', '0.1.0')
    DESCRIPTION: str = os.getenv('DESCRIPTION', 'FastAPI service for receiving results from Resolve scenario runs')
    API_HOST: str = os.getenv('API_HOST', '0.0.0.0')
    API_PORT: int = int(os.getenv('API_PORT', '8080'))
    API_V1_STR: str = '/api/v1'

    # Database
    DATABASE_URL: str | None = os.getenv("DATABASE_URL")
    POSTGRES_DB: str = os.getenv("POSTGRES_DB", "prodcast2")
    POSTGRES_USER: str = os.getenv("POSTGRES_USER", "prodcast")
    POSTGRES_PASSWORD: str = os.getenv("POSTGRES_PASSWORD", "")
    POSTGRES_HOST: str = os.getenv("POSTGRES_HOST", "db")
    POSTGRES_PORT: str = os.getenv("POSTGRES_PORT", "5432")

    # Worker / Paths
    MODEL_DOWNLOAD_TOKEN: str | None = os.getenv('MODEL_DOWNLOAD_TOKEN')
    WORKER_CALLBACK_TOKEN: str | None = os.getenv('WORKER_CALLBACK_TOKEN')
    CALLBACK_URL: str | None = os.getenv('CALLBACK_URL')
    SERVER_NAME: str = os.getenv('SERVER_NAME', 'Worker')
    WORK_LOGS_DIR: str = os.getenv('WORK_LOGS_DIR', 'WorkServerLogs')
    WORK_DATA_DIR: str = os.getenv('WORK_DATA_DIR', 'WorkServerData')



settings = Settings()
