# from sqlalchemy import create_engine
# from sqlalchemy.orm import sessionmaker

# from core.config import settings

# engine = create_engine(
#     f'mssql+pymssql://{settings.DJANGO_USER}:{settings.DB_PASSWORD}@{settings.DB_SERVER}/{settings.DB_NAME}'
# )
# Session = sessionmaker(bind=engine)


from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
import urllib.parse

from resolve_api.core.config import settings


def build_postgres_uri() -> str:
    if settings.DATABASE_URL:
        return settings.DATABASE_URL

    user = urllib.parse.quote_plus(settings.POSTGRES_USER)
    pwd = urllib.parse.quote_plus(settings.POSTGRES_PASSWORD)
    host = settings.POSTGRES_HOST
    port = settings.POSTGRES_PORT
    db = settings.POSTGRES_DB
    return f"postgresql+psycopg://{user}:{pwd}@{host}:{port}/{db}"


DATABASE_URI = build_postgres_uri()

engine = create_engine(
    DATABASE_URI,
    pool_pre_ping=True,
    future=True,
)

Session = sessionmaker(bind=engine, autoflush=False, autocommit=False, expire_on_commit=False)
