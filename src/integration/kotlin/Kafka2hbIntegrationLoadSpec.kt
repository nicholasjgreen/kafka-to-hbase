import io.kotlintest.specs.StringSpec
import lib.getISO8601Timestamp
import lib.sendRecord
import lib.wellFormedValidPayload
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.log4j.Logger

class Kafka2hbIntegrationLoadSpec : StringSpec() {

    private val log = Logger.getLogger(Kafka2hbIntegrationLoadSpec::class.toString())
    private val maxRecords = 1000

    init {
        "Send many messages for a load test" {
            val producer = KafkaProducer<ByteArray, ByteArray>(Config.Kafka.producerProps)
            val converter = Converter()
            val collectionName = "load-test"
            val dbName = "test-db"
            val topic = "db.$collectionName.$dbName"

            for (recordNumber in 1..maxRecords) {
                val body = wellFormedValidPayload(collectionName, dbName)
                val timestamp = converter.getTimestampAsLong(getISO8601Timestamp())

                log.info("Sending well-formed record $recordNumber/$maxRecords to kafka topic '$topic'.")
                producer.sendRecord(
                    topic.toByteArray(),
                    "key-$recordNumber-of-$maxRecords".toByteArray(),
                    body,
                    timestamp
                )
                log.info("Sent well-formed record $recordNumber/$maxRecords to kafka topic '$topic'.")
            }
        }
    }

}
