""" Configuration and environment manipulation """

import re


def delimited(d):
    """ Parse the value as a delimited string """
    return lambda v: v.split(",") if v else d


def string(d):
    """ Parse the value as a string """
    return lambda v: v or d


def integer(d):
    """ Parse the value as an integer """
    return lambda v: int(v) if v else d


def boolean(d):
    """ Parse the value as a boolean literal """
    return lambda v: v.lower() == "true" if v else d


default_config = {
    "kafka.bootstrap_servers": delimited([]),
    "kafka.client_id": string("kafka-to-hbase"),
    "kafka.group_id": string(""),
    "kafka.fetch_min_bytes": integer(1),
    "kafka.fetch_max_wait_ms": integer(500),
    "kafka.fetch_max_bytes": integer(50 * 1024 * 1024),
    "kafka.max_partition_fetch_bytes": integer(1024 * 1024),
    "kafka.request_timeout_ms": integer(305000),
    "kafka.retry_backoff_ms": integer(100),
    "kafka.reconnect_backoff_ms": integer(1000),
    "kafka.max_in_flight_requests_per_connection": integer(5),
    "kafka.check_crcs": boolean(True),
    "kafka.metadata_max_age": integer(300000),
    "kafka.session_timeout_ms": integer(10000),
    "kafka.heartbeat_interval_ms": integer(3000),
    "kafka.receive_buffer_bytes": integer(32 * 1024),
    "kafka.send_buffer_bytes": integer(128 * 1024),
    "kafka.consumer_timeout_ms": integer(0),
    "kafka.conections_max_idle_ms": integer(540000),
    "kafka.ssl": boolean(True),
    "kafka.ssl_check_hostname": boolean(True),
    "kafka.ssl_cafile": string(""),
    "kafka.ssl_certfile": string(""),
    "kafka.ssl_keyfile": string(""),
    "kafka.ssl_password": string(""),
    "kafka.ssl_crlfile": string(""),
    "kafka.ssl_ciphers": string(""),
    "kafka.sasl_kerberos": boolean(False),
    "kafka.sasl_kerberos_service_name": string("kafka"),
    "kafka.sasl_kerberos_domain_name": string(""),
    "kafka.sasl_plain_username": string(""),
    "kafka.sasl_plain_password": string(""),
    "kafka.topics": delimited([]),
    "hbase.host": string("localhost"),
    "hbase.port": integer(9090),
    "hbase.timeout": integer(0),
    "hbase.namespace": string(""),
    "hbase.table_prefix": string(""),
    "hbase.column": string(""),
}


def env_var_for_config_key(k):
    """ Calculate the K2HB env var for a given config key """
    return "K2HB_" + re.sub("\\W+", "_", k).upper()


def load_config(environment):
    """ Load environment variables into usable configuration from the given collection """
    config = {}

    for path, factory in default_config.items():
        env_var = env_var_for_config_key(path)
        group, key = path.split(".", 1)
        config[group] = config.get(group, {})
        config[group][key] = factory(environment.get(env_var))

    return config


def all_env_vars():
    """ Env vars and defaults """

    def _get_value(f):
        v = f(None)
        if isinstance(v, list):
            v = ",".join(v)

    return {
        env_var_for_config_key(k): _get_value(f)
        for k, f in default_config.items()
    }
