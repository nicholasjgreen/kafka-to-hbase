import org.apache.hadoop.conf.Configuration
import org.apache.kafka.common.serialization.ByteArrayDeserializer
import org.apache.kafka.common.serialization.ByteArraySerializer
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
            // See also https://hbase.apache.org/book.html#hbase_default_configurations
            set("zookeeper.znode.parent", getEnv("K2HB_HBASE_ZOOKEEPER_PARENT") ?: "/hbase")
            set("hbase.zookeeper.quorum", getEnv("K2HB_HBASE_ZOOKEEPER_QUORUM") ?: "zookeeper")
            setInt("hbase.zookeeper.port", getEnv("K2HB_HBASE_ZOOKEEPER_PORT")?.toIntOrNull() ?: 2181)
        }

        val dataTable = getEnv("K2HB_HBASE_DATA_TABLE") ?: "k2hb:ingest"
        val dataFamily = getEnv("K2HB_HBASE_DATA_FAMILY") ?: "topic"
        val topicTable = getEnv("K2HB_HBASE_TOPIC_TABLE") ?: "k2hb:ingest-topic"
        val topicFamily = getEnv("K2HB_HBASE_TOPIC_FAMILY") ?: "c"
        val topicQualifier = getEnv("K2HB_HBASE_TOPIC_QUALIFIER") ?: "msg"
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

            put("key.serializer", ByteArraySerializer::class.java)
            put("value.serializer", ByteArraySerializer::class.java)

            put("auto.offset.reset", "earliest")
        }

        val pollTimeout: Duration = getEnv("K2HB_KAFKA_POLL_TIMEOUT")?.toDuration() ?: Duration.ofDays(10)
        val topics = getEnv("K2HB_KAFKA_TOPICS")?.split(',') ?: listOf("test-topic")
    }
}