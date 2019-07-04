import kotlinx.coroutines.GlobalScope
import kotlinx.coroutines.async
import kotlinx.coroutines.isActive
import org.apache.kafka.clients.consumer.KafkaConsumer
import java.time.Duration

fun shovelAsync(kafka: KafkaConsumer<ByteArray, ByteArray>, hbase: HbaseClient) = GlobalScope.async {
    while (isActive) {
        var records = kafka.poll(Duration.ofSeconds(10))
        for (record in records) {
            hbase.putVersion(
                topic = record.topic().toByteArray(),
                key = record.key(),
                body = record.value(),
                version = record.timestamp()
            )
        }

        if (!records.isEmpty) {
            kafka.commitSync()
        }
    }
}