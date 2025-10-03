import os
import django
from django.conf import settings


def setup_django():
    """
    Настраиваем Django окружение для работы с моделями воркера.
    """
    if settings.configured:
        return

    settings.configure(
        DATABASES = {
            'default': {
                'ENGINE': 'mssql',
                'NAME': 'DOFGI1',
                'HOST': 'KPCDBS14\\CYRGEN',
                'OPTIONS': {
                    'driver': 'ODBC Driver 17 for SQL Server',
                    'trusted_connection': 'yes',
                },
            },
        },
        INSTALLED_APPS=[
            "worker",  # регистрируем свои модели
        ],
        TIME_ZONE="Asia/Almaty",
        USE_TZ=True,
    )
    django.setup()
