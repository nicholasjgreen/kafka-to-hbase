import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.isActive
import org.apache.kafka.clients.consumer.KafkaConsumer
import java.time.Duration
import java.time.LocalDateTime

fun KafkaConsumer<ByteArray, ByteArray>.consume(
    pollDuration: Duration,
    maxQuietDuration: Duration,
    context: CoroutineScope
) = sequence {
    var startTime = LocalDateTime.now()

    while (context.isActive) {
        val records = poll(pollDuration)

        if (records.isEmpty) {
            if (Duration.between(startTime, LocalDateTime.now()) > maxQuietDuration) break
            continue
        }

        for (record in records) {
            yield(
                Record(
                    topic = record.topic().toByteArray(),
                    key = record.key(),
                    value = record.value(),
                    timestamp = record.timestamp(),
                    partition = record.partition(),
                    offset = record.offset()
                )
            )
        }

        commitAsync()
    }
}
