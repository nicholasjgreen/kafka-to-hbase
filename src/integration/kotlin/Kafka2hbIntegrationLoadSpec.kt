import io.kotlintest.shouldNotBe
import io.kotlintest.specs.StringSpec
import lib.*
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.log4j.Logger

class Kafka2hbIntegrationLoadSpec: StringSpec(){

    private val log = Logger.getLogger(Kafka2hbIntegrationLoadSpec::class.toString())
    private val maxRecords = 1000
    init {
        "Send many messages for a load test" {
            val hbase = HbaseClient.connect()
            val producer = KafkaProducer<ByteArray, ByteArray>(Config.Kafka.producerProps)
            val converter = Converter()
            val topic = "db.load.test-data"
            val matcher = TextUtils().topicNameTableMatcher(topic)
            matcher shouldNotBe null
            if (matcher != null) {
                val namespace = matcher.groupValues[1]
                val tableName = matcher.groupValues[2]
                val qualifiedTableName = "$namespace:$tableName".replace("-", "_")
                hbase.ensureTable(qualifiedTableName)

                for (x in 1..maxRecords) {
                    val body = wellformedValidPayload()
                    val timestamp = converter.getTimestampAsLong(getISO8601Timestamp())

                    log.info("Sending well-formed record $x/$maxRecords to kafka topic '$topic'.")
                    producer.sendRecord(topic.toByteArray(), "key1".toByteArray(), body, timestamp)
                    log.info("Sent well-formed record $x/$maxRecords to kafka topic '$topic'.")
                }
            }
        }
    }

}
