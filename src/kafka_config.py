import os
from typing import Any, Dict


def kafka_client_config_from_env() -> Dict[str, Any]:
    """
    Build a confluent-kafka client config from environment variables.

    Local defaults (no auth):
      - KAFKA_BOOTSTRAP_SERVERS=localhost:9092

    Confluent Cloud (SASL/SSL):
      - KAFKA_BOOTSTRAP_SERVERS=...
      - KAFKA_SECURITY_PROTOCOL=SASL_SSL
      - KAFKA_SASL_MECHANISM=PLAIN
      - KAFKA_SASL_USERNAME=<API KEY>
      - KAFKA_SASL_PASSWORD=<API SECRET>
    """

    cfg: Dict[str, Any] = {
        "bootstrap.servers": os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
    }

    security_protocol = os.getenv("KAFKA_SECURITY_PROTOCOL")
    sasl_mechanism = os.getenv("KAFKA_SASL_MECHANISM")
    sasl_username = os.getenv("KAFKA_SASL_USERNAME")
    sasl_password = os.getenv("KAFKA_SASL_PASSWORD")

    if security_protocol:
        cfg["security.protocol"] = security_protocol
    if sasl_mechanism:
        cfg["sasl.mechanism"] = sasl_mechanism
    if sasl_username:
        cfg["sasl.username"] = sasl_username
    if sasl_password:
        cfg["sasl.password"] = sasl_password

    # Recommended for producers (safe for consumers too; ignored where not applicable)
    if os.getenv("KAFKA_ENABLE_IDEMPOTENCE", "true").lower() in {"1", "true", "yes", "y"}:
        cfg["enable.idempotence"] = True

    # If user wants to override with raw librdkafka props, allow KAFKA_EXTRA_CONFIG__FOO=bar
    # Example: KAFKA_EXTRA_CONFIG__debug=broker,security
    prefix = "KAFKA_EXTRA_CONFIG__"
    for k, v in os.environ.items():
        if k.startswith(prefix):
            cfg_key = k[len(prefix) :].replace("__", ".")
            cfg[cfg_key] = v

    return cfg

