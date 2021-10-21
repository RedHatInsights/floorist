from app_common_python import LoadedConfig, ObjectBuckets
from app_common_python import isClowderEnabled
from os import environ, access, R_OK
from os.path import isfile


import attr


@attr.s
class Config:
    bucket_url=attr.ib(default=None)
    bucket_name=attr.ib(default=None)
    bucket_secret_key=attr.ib(default=None)
    bucket_access_key=attr.ib(default=None)
    bucket_region=attr.ib(default=None)
    database_hostname=attr.ib(default=None)
    database_username=attr.ib(default=None)
    database_password=attr.ib(default=None)
    database_name=attr.ib(default=None)
    floorplan_filename=attr.ib(default=None)


def get_config():

    config = Config()
    _set_bucket_config(config)
    _set_database_config(config)
    _set_floorist_config(config)
    _validate_config(config)

    return config


def _set_bucket_config(config):

    if not isClowderEnabled() or force_environment_bucket_config():
        _set_bucket_config_from_environment(config)
    else:
        _set_bucket_config_from_clowder(config)


def force_environment_bucket_config():
    return environ.get("FORCE_ENVIRONMENT_BUCKET_CONFIG") is not None


def _set_bucket_config_from_environment(config):
    config.bucket_name = get_bucket_requested_name_from_environment()
    config.bucket_url = environ.get('AWS_ENDPOINT')
    config.bucket_secret_key = environ.get('AWS_SECRET_ACCESS_KEY')
    config.bucket_access_key = environ.get('AWS_ACCESS_KEY_ID')
    config.bucket_region = environ.get('AWS_REGION')


def _set_bucket_config_from_clowder(config):

    bucket_requested_name = get_bucket_requested_name_from_environment()
    object_store_config = LoadedConfig.objectStore
    bucket_config = ObjectBuckets[bucket_requested_name]
    config.bucket_name = bucket_config.name
    config.bucket_url = _get_bucket_url(object_store_config.hostname, object_store_config.port, object_store_config.tls)
    config.bucket_access_key = bucket_config.accessKey
    config.bucket_secret_key = bucket_config.secretKey
    config.bucket_region = bucket_config.region


def _get_bucket_url(hostname, port, https):

    protocol = "https" if https else "http"
    return f"{protocol}://{hostname}:{port}"


def get_bucket_requested_name_from_environment():

    name = environ.get('AWS_BUCKET')
    if not name:
        raise ValueError("Bucket name not configured, set AWS_BUCKET variable.")

    return name


def _set_database_config(config):

    if isClowderEnabled():
        _set_database_config_from_clowder(config)
    else:
        _set_database_config_from_environment(config)


def _set_database_config_from_clowder(config):
    database_config = LoadedConfig.database
    config.database_name = database_config.name
    config.database_hostname = database_config.hostname
    config.database_username = database_config.username
    config.database_password = database_config.password


def _set_database_config_from_environment(config):
    config.database_hostname = environ.get('POSTGRES_SERVICE_HOST')
    config.database_name = environ.get('POSTGRESQL_DATABASE')
    config.database_username = environ.get('POSTGRESQL_USER')
    config.database_password = environ.get('POSTGRESQL_PASSWORD')


def _set_floorist_config(config):
    config.floorplan_filename = environ.get('FLOORPLAN_FILE')


def _validate_config(config):

    if not config.floorplan_filename:
        raise ValueError("Floorplan filename not defined!")

    if not isfile(config.floorplan_filename) or not access(config.floorplan_filename, R_OK):
        raise IOError(f"File '{config.floorplan_filename}' does not exist or is not readable")

    if not config.database_hostname:
        raise ValueError("Database host not defined")

    if not config.database_name:
        raise ValueError("Database name not defined")

    if not config.database_username:
        raise ValueError("Database user not defined")

    if not config.database_password:
        raise ValueError("Database password not defined")
