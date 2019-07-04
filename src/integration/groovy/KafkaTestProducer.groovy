import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.BytesSerializer
import org.apache.kafka.common.utils.Bytes

class KafkaTestProducer {
    KafkaProducer<byte[], byte[]> producer

    KafkaTestProducer() {
        def props = new Properties()
        props.with {
            put "bootstrap.servers", "kafka:9092"
            put "key.serializer", BytesSerializer
            put "value.serializer", BytesSerializer
            put "api.version.request", false
        }

        producer = new KafkaProducer<byte[], byte[]>(props)
    }

    def sendRecord(byte[] topic, byte[] key, byte[] body, long timestamp) {
        def record = new ProducerRecord(
                new String(topic),
                null,
                timestamp,
                Bytes.wrap(key),
                Bytes.wrap(body),
                null
        )


        try {
            producer.send record
        } finally {
            producer.flush()
        }
    }
}
