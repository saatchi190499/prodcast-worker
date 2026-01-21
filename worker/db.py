import os
import django
from django.conf import settings


def setup_django():
    """
    Настраиваем Django окружение для работы с моделями воркера.
    """
    if settings.configured:
        return

    base_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
    media_root = os.environ.get("MEDIA_ROOT", os.path.join(base_dir, "media"))

    settings.configure(
        DATABASES={
            "default": {
                "ENGINE": "django.db.backends.postgresql",
                "NAME": os.getenv("POSTGRES_DB", "prodcast2"),
                "USER": os.getenv("POSTGRES_USER", "postgres"),
                "PASSWORD": os.getenv("POSTGRES_PASSWORD", "1"),
                "HOST": os.getenv("POSTGRES_HOST", "postgresql"),
                "PORT": os.getenv("POSTGRES_PORT", "5432"),
            },
        },
        INSTALLED_APPS=[
            "django.contrib.contenttypes",
            "django.contrib.auth",
            "smart_selects",
            "worker",  # регистрируем свои модели
        ],
        TIME_ZONE="Asia/Almaty",
        USE_TZ=True,
        MEDIA_URL="/media/",
        MEDIA_ROOT=media_root,
    )
    django.setup()
