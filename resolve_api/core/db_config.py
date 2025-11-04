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

from app.core.config import settings


def build_mssql_uri() -> str:
    """
    Формирует корректный ODBC URI для SQLAlchemy с учётом Trusted_Connection.
    """
    parts = [
        f"DRIVER={{{settings.DB_DRIVER}}}",
        f"SERVER={settings.DB_SERVER}",
        f"DATABASE={settings.DB_NAME}",
    ]

    if settings.DB_TRUSTED or not getattr(settings, "DB_USER", None):
        parts.append("Trusted_Connection=yes")
    else:
        user = getattr(settings, "DB_USER", "")
        pwd = getattr(settings, "DB_PASSWORD", "")
        parts += [f"UID={user}", f"PWD={pwd}"]

    if settings.DB_TRUST_SERVER_CERT:
        parts.append("TrustServerCertificate=yes")
    if settings.DB_ENCRYPT:
        parts.append("Encrypt=yes")

    odbc_str = ";".join(parts) + ";"
    return "mssql+pyodbc:///?odbc_connect=" + urllib.parse.quote_plus(odbc_str)


DATABASE_URI = build_mssql_uri()

engine = create_engine(
    DATABASE_URI,
    pool_pre_ping=True,
    fast_executemany=True,
    future=True,
)

Session = sessionmaker(bind=engine, autoflush=False, autocommit=False, expire_on_commit=False)
