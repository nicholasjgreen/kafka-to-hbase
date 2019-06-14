""" Configuration and environment manipulation """

from collections import namedtuple


def delimited(d):
    """ Parse the value as a delimited string """
    return lambda v: [x for x in (v or d).split(",") if x]


def string(d):
    """ Parse the value as a string """
    return lambda v: v or d


def integer(d):
    """ Parse the value as an integer """
    return lambda v: int(v or d)


def boolean(d):
    """ Parse the value as a boolean literal """
    return lambda v: v.lower() == "true" if v else d


class Config(namedtuple("ConfigTuple", ["kafka", "hbase"])):
    """ Config class for K2HB """

    def as_dict(self):
        """ Return the config as a set of nested dicts """
        return {
            g: {
                k: v
                for k, v in n._asdict().items()
            }
            for g, n in self._asdict().items()
        }


default_config = {
    "kafka": {
        "bootstrap_servers": delimited("localhost:9092"),
        "client_id": string("kafka-to-hbase"),
        "group_id": string("kafka-to-hbase"),
        "fetch_min_bytes": integer(1),
        "fetch_max_wait_ms": integer(500),
        "fetch_max_bytes": integer(50 * 1024 * 1024),
        "max_partition_fetch_bytes": integer(1024 * 1024),
        "request_timeout_ms": integer(305000),
        "retry_backoff_ms": integer(100),
        "reconnect_backoff_ms": integer(1000),
        "max_in_flight_requests_per_connection": integer(5),
        "check_crcs": boolean(True),
        "metadata_max_age": integer(300000),
        "session_timeout_ms": integer(10000),
        "heartbeat_interval_ms": integer(3000),
        "receive_buffer_bytes": integer(32 * 1024),
        "send_buffer_bytes": integer(128 * 1024),
        "consumer_timeout_ms": integer(0),
        "conections_max_idle_ms": integer(540000),
        "ssl": boolean(False),
        "ssl_check_hostname": boolean(True),
        "ssl_cafile": string(""),
        "ssl_certfile": string(""),
        "ssl_keyfile": string(""),
        "ssl_password": string(""),
        "ssl_crlfile": string(""),
        "ssl_ciphers": string(""),
        "sasl_kerberos": boolean(False),
        "sasl_kerberos_service_name": string("kafka"),
        "sasl_kerberos_domain_name": string(""),
        "sasl_plain_username": string(""),
        "sasl_plain_password": string(""),
        "topics": delimited(""),
    },
    "hbase": {
        "host": string("localhost"),
        "port": integer(9090),
        "timeout": integer(0),
        "namespace": string(""),
        "table_prefix": string(""),
        "column": string(""),
    },
}

KafkaConfig = namedtuple("KafkaConfig", default_config["kafka"].keys())  # type: ignore
HbaseConfig = namedtuple("HbaseConfig", default_config["hbase"].keys())  # type: ignore


def env_var_for_config_key(g, k):
    """ Calculate the K2HB env var for a given config key """
    return "K2HB_{}_{}".format(g.upper(), k.upper())


def load_config(environment):
    """ Load environment variables into usable configuration from the given collection """
    config = {}

    for group in default_config:
        for key, factory in default_config[group].items():
            env_var = env_var_for_config_key(group, key)
            config[group] = config.get(group, {})
            config[group][key] = factory(environment.get(env_var))

    return Config(kafka=KafkaConfig(**config["kafka"]), hbase=HbaseConfig(**config["hbase"]))


def all_env_vars(environment):
    """ Env vars and defaults """

    def _get_value(g, k, f):
        env_var = env_var_for_config_key(g, k)
        v = f(environment.get(env_var))

        if isinstance(v, list):
            v = ",".join(v)

        if isinstance(v, bool):
            v = str(v).lower()

        v = str(v)

        return (env_var, v)

    return dict(
        _get_value(g, k, f)
        for g in default_config
        for k, f in default_config[g].items()
    )
