import com.nhaarman.mockitokotlin2.*
import io.kotlintest.shouldThrow
import io.kotlintest.specs.StringSpec
import kotlinx.coroutines.runBlocking
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.consumer.ConsumerRecords
import org.apache.kafka.clients.consumer.KafkaConsumer

import java.time.Duration
import java.util.logging.Logger

class ShovelTest : StringSpec() {

    init {
        "Exception thrown in async task is re-thrown in parent" {
            val hbase = mock<HbaseClient> {
                on { putVersion(any(), any(), any(), any()) } doThrow java.io.IOException()
            }


            val consumerRecord = mock<ConsumerRecord<ByteArray, ByteArray>> {
                on { value() } doReturn """
                    {
                       "message": {
                           "@type": "hello",
                           "_id": {
                               "declarationId": 1
                           },
                           "_lastModifiedDateTime": "2019-07-04T07:27:35.104+0000",
                           "collection" : "addresses",
                           "db": "core",
                           "dbObject": "asd",
                           "encryption": {
                               "keyEncryptionKeyId": "cloudhsm:7,14",
                               "initialisationVector": "iv",
                               "encryptedEncryptionKey": "=="
                           }
                       }
                    }
                """.trimIndent().toByteArray()
                on { key() } doReturn "TestKey".toByteArray()
                on { topic() } doReturn "testTopic"
            }

            val consumerRecordIter = mock<MutableIterator<ConsumerRecord<ByteArray, ByteArray>>> {
                on { next() } doReturn consumerRecord
                on { hasNext() }.doReturn(true, false)
            }

            val consumerRecords = mock<ConsumerRecords<ByteArray, ByteArray>> {
                on { count() } doReturn 1
                on { iterator() } doReturn consumerRecordIter
            }

            val consumer = mock<KafkaConsumer<ByteArray, ByteArray>> {
                on { poll(any<Duration>()) } doReturn consumerRecords
            }

            shouldThrow<java.io.IOException> {
                val job = shovelAsync(consumer, hbase, Duration.ofSeconds(10))
                job.await()
            }

        }
    }

}
