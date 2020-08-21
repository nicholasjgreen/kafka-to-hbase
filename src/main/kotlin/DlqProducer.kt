import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.Producer


object DlqProducer {
    private var INSTANCE: KafkaProducer<ByteArray, ByteArray>? = null
    @Synchronized
    fun getInstance(): Producer<ByteArray, ByteArray> {
        if (INSTANCE == null) {
            INSTANCE = KafkaProducer(Config.Kafka.producerProps)
        }

        return INSTANCE!!
    }
}
