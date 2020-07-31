package lib

import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.log4j.Logger

private val log = Logger.getLogger(KafkaProducer::class.toString())

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
