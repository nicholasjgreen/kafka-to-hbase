import org.apache.hadoop.conf.Configuration
import org.apache.kafka.common.serialization.ByteArrayDeserializer
import java.time.Duration
import java.util.*

fun getEnv(envVar: String): String? {
    val value = System.getenv(envVar)
    return if (value.isNullOrEmpty()) null else value
}

fun String.toDuration(): Duration {
    return Duration.parse(this)
}

object Config {
    object Hbase {
        val config = Configuration().apply {
            set("hbase.zookeeper.quorum", getEnv("K2HB_HBASE_ZOOKEEPER_QUORUM") ?: "zookeeper")
            setInt("hbase.zookeeper.port", getEnv("K2HB_HBASE_ZOOKEEPER_PORT")?.toIntOrNull() ?: 2181)
        }

        val namespace = getEnv("K2HB_HBASE_NAMESPACE") ?: "k2hb"
        val family = getEnv("K2HB_HBASE_FAMILY_NAME") ?: "cf"
        val qualifier = getEnv("K2HB_HBASE_QUALIFIER") ?: "data"
        val minVersions = getEnv("K2HB_HBASE_FAMILY_MIN_VERSIONS")?.toInt() ?: 1
        val maxVersions = getEnv("K2HB_HBASE_FAMILY_MAX_VERSIONS")?.toInt() ?: 10
        val TTL: Duration = getEnv("K2HB_HBASE_FAMILY_TTL")?.toDuration() ?: Duration.ofDays(10)
    }

    object Kafka {
        val props = Properties().apply {
            put("bootstrap.servers", getEnv("K2HB_KAFKA_BOOTSTRAP_SERVERS") ?: "kafka:9092")
            put("group.id", getEnv("K2HB_KAFKA_CONSUMER_GROUP") ?: "test")

            val useSSL = getEnv("K2HB_KAFKA_INSECURE") != "true"
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
        }

        val pollTimeout: Duration = getEnv("K2HB_KAFKA_POLL_TIMEOUT")?.toDuration() ?: Duration.ofDays(10)
        val topics = getEnv("K2HB_KAFKA_TOPICS")?.split(',') ?: listOf("test-topic")
    }
}