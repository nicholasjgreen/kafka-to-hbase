package lib

import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerRecord

fun <K : Any?, V : Any?> KafkaProducer<K, V>.sendRecord(topic: ByteArray, key: K, body: V, timestamp: Long) {
    val record = ProducerRecord(
        String(topic),
        null,
        timestamp,
        key,
        body,
        null
    )

    try {
        send(record)
    } finally {
        flush()
    }
}
