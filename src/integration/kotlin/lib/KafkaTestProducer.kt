package lib

import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.ByteArraySerializer
import java.util.*

class KafkaTestProducer {
    private val producer: KafkaProducer<ByteArray, ByteArray> = KafkaProducer(
        Properties().apply {
            put("bootstrap.servers", "kafka:9092")
            put("key.serializer", ByteArraySerializer::class.java)
            put("value.serializer", ByteArraySerializer::class.java)
            put("api.version.request", false)
        }
    )

    fun sendRecord(topic: ByteArray, key: ByteArray, body: ByteArray, timestamp: Long) {
        val record = ProducerRecord(
            String(topic),
            null,
            timestamp,
            key,
            body,
            null
        )

        try {
            producer.send(record)
        } finally {
            producer.flush()
        }
    }
}
