import org.apache.hadoop.conf.Configuration
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.ByteArrayDeserializer
import org.apache.kafka.common.serialization.ByteArraySerializer
import java.io.File
import java.time.Duration
import java.util.*
import java.util.regex.Pattern


fun getEnv(envVar: String): String? {
    val value = System.getenv(envVar)
    return if (value.isNullOrEmpty()) null else value
}

fun String.toDuration(): Duration {
    return Duration.parse(this)
}

fun readFile(fileName: String): String
        = File(fileName).readText(Charsets.UTF_8)

object Config {

    const val metaDataRefreshKey = "metadata.max.age.ms"

    object Shovel {
        val reportFrequency = getEnv("K2HB_KAFKA_REPORT_FREQUENCY")?.toInt() ?: 100
    }

    object Validator {
        val properties = Properties().apply {
            put("schema.location", getEnv("K2HB_VALIDATOR_SCHEMA") ?: "message.schema.json")
        }
    }

    object Hbase {
        val config = Configuration().apply {
            set("zookeeper.znode.parent", getEnv("K2HB_HBASE_ZOOKEEPER_PARENT") ?: "/hbase")
            set("hbase.zookeeper.quorum", getEnv("K2HB_HBASE_ZOOKEEPER_QUORUM") ?: "zookeeper")
            setInt("hbase.zookeeper.port", getEnv("K2HB_HBASE_ZOOKEEPER_PORT")?.toIntOrNull() ?: 2181)
            set("hbase.rpc.timeout", getEnv("K2HB_HBASE_RPC_TIMEOUT_MILLISECONDS") ?: "1200000")
            set("hbase.client.operation.timeout", getEnv("K2HB_HBASE_OPERATION_TIMEOUT_MILLISECONDS") ?: "1800000")
            set("hbase.client.pause", getEnv("K2HB_HBASE_PAUSE_MILLISECONDS") ?: "50")
            set("hbase.client.retries.number", getEnv("K2HB_HBASE_RETRIES") ?: "50")
        }

        val columnFamily = getEnv("K2HB_HBASE_COLUMN_FAMILY") ?: "cf"
        val columnQualifier = getEnv("K2HB_HBASE_COLUMN_QUALIFIER") ?: "record"
        val retryMaxAttempts: Int = getEnv("K2HB_RETRY_MAX_ATTEMPTS")?.toInt() ?: 3
        val retryInitialBackoff: Long = getEnv("K2HB_RETRY_INITIAL_BACKOFF")?.toLong() ?: 10000
        val retryBackoffMultiplier: Long = getEnv("K2HB_RETRY_BACKOFF_MULTIPLIER")?.toLong() ?: 2
        val regionReplication: Int = getEnv("K2HB_HBASE_REGION_REPLICATION")?.toInt() ?: 3
        val logKeys: Boolean = getEnv("K2HB_HBASE_LOG_KEYS")?.toBoolean() ?: true
        val cleanExit: Boolean = getEnv("K2HB_HBASE_CLEAN_EXIT")?.toBoolean() ?: true
    }

    object Kafka {
        val consumerProps = Properties().apply {
            put("bootstrap.servers", getEnv("K2HB_KAFKA_BOOTSTRAP_SERVERS") ?: "kafka:9092")
            put("group.id", getEnv("K2HB_KAFKA_CONSUMER_GROUP") ?: "test")

            val sslVal = getEnv("K2HB_KAFKA_INSECURE") ?: "true"
            val useSSL = sslVal != "true"
            if (useSSL) {
                put("security.protocol", "SSL")
                put("ssl.truststore.location", getEnv("K2HB_TRUSTSTORE_PATH"))
                put("ssl.truststore.password", getEnv("K2HB_TRUSTSTORE_PASSWORD"))
                put("ssl.keystore.location", getEnv("K2HB_KEYSTORE_PATH"))
                put("ssl.keystore.password", getEnv("K2HB_KEYSTORE_PASSWORD"))
                put("ssl.key.password", getEnv("K2HB_PRIVATE_KEY_PASSWORD"))
            }

            put("key.deserializer", ByteArrayDeserializer::class.java)
            put("value.deserializer", ByteArrayDeserializer::class.java)

            put("auto.offset.reset", "earliest")
            put(metaDataRefreshKey, getEnv("K2HB_KAFKA_META_REFRESH_MS") ?: "10000")
            put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false)
            put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, getEnv("K2HB_KAFKA_MAX_POLL_RECORDS") ?: 500)
            val pollInterval = getEnv("K2HB_KAFKA_MAX_POLL_INTERVAL_MS")
            if (pollInterval != null) {
                put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, pollInterval.toInt())
            }
        }

        val producerProps = Properties().apply {
            put("bootstrap.servers", getEnv("K2HB_KAFKA_BOOTSTRAP_SERVERS") ?: "kafka:9092")

            val sslVal = getEnv("K2HB_KAFKA_INSECURE") ?: "true"
            val useSSL = sslVal != "true"
            if (useSSL) {
                put("security.protocol", "SSL")
                put("ssl.truststore.location", getEnv("K2HB_TRUSTSTORE_PATH"))
                put("ssl.truststore.password", getEnv("K2HB_TRUSTSTORE_PASSWORD"))
                put("ssl.keystore.location", getEnv("K2HB_KEYSTORE_PATH"))
                put("ssl.keystore.password", getEnv("K2HB_KEYSTORE_PASSWORD"))
                put("ssl.key.password", getEnv("K2HB_PRIVATE_KEY_PASSWORD"))
            }

            put("key.serializer", ByteArraySerializer::class.java)
            put("value.serializer", ByteArraySerializer::class.java)
            put(metaDataRefreshKey, getEnv("K2HB_KAFKA_META_REFRESH_MS") ?: "10000")
        }

        val pollTimeout: Duration = getEnv("K2HB_KAFKA_POLL_TIMEOUT")?.toDuration() ?: Duration.ofSeconds(3)
        var topicRegex: Pattern = Pattern.compile(getEnv("K2HB_KAFKA_TOPIC_REGEX") ?: "db.*")
        var dlqTopic = getEnv("K2HB_KAFKA_DLQ_TOPIC") ?: "test-dlq-topic"

        fun metadataRefresh(): String = consumerProps.getProperty(metaDataRefreshKey)
    }

    object MetadataStore {

        private val useAwsSecretsString = getEnv("K2HB_USE_AWS_SECRETS") ?: "true"
        val isUsingAWS = useAwsSecretsString == "true"

        val properties = Properties().apply {
            put("user", getEnv("K2HB_RDS_USERNAME") ?: "user")
            put("rds.password.secret.name", getEnv("K2HB_RDS_PASSWORD_SECRET_NAME") ?: "metastore_password")
            put("database", getEnv("K2HB_RDS_DATABASE_NAME") ?: "database")
            put("rds.endpoint", getEnv("K2HB_RDS_ENDPOINT") ?: "127.0.0.1")
            put("rds.port", getEnv("K2HB_RDS_PORT") ?: "3306")
            put("use.aws.secrets", getEnv("K2HB_USE_AWS_SECRETS") ?: "true")

            if (isUsingAWS) {
                put("ssl_ca_path", getEnv("K2HB_RDS_CA_CERT_PATH") ?: "/certs/AmazonRootCA1.pem")
                put("ssl_ca", readFile(getProperty("ssl_ca_path")))
                put("ssl_verify_cert", true)
            }
        }
    }

    object SecretManager {
        val properties = Properties().apply {
            put("region", getEnv("SECRET_MANAGER_REGION") ?: "eu-west-2")
        }
    }
}
