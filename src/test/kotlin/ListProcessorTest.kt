import com.nhaarman.mockitokotlin2.*
import io.kotlintest.shouldBe
import io.kotlintest.shouldNotBe
import io.kotlintest.specs.StringSpec
import org.apache.hadoop.hbase.util.Bytes
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.consumer.ConsumerRecords
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.consumer.OffsetAndMetadata
import org.apache.kafka.common.TopicPartition
import java.io.IOException


class ListProcessorTest : StringSpec() {

    init {
        "Only commits offsets on success, resets position on failure" {
            val processor = ListProcessor(mock(), Converter())
            val hbaseClient = hbaseClient()
            val consumer = kafkaConsumer()
            processor.processRecords(hbaseClient, consumer, messageParser(),  consumerRecords())
            verifyHbaseInteractions(hbaseClient)
            verifyKafkaInteractions(consumer)
        }
    }

    private fun verifyHbaseInteractions(hbaseClient: HbaseClient) {
        verifyHBasePuts(hbaseClient)
        verifyNoMoreInteractions(hbaseClient)
    }

    private fun verifyHBasePuts(hbaseClient: HbaseClient) {
        val tableNameCaptor = argumentCaptor<String>()
        val recordCaptor = argumentCaptor<List<HbasePayload>>()
        verify(hbaseClient, times(10)).putList(tableNameCaptor.capture(), recordCaptor.capture())
        tableNameCaptor.allValues shouldBe (1..10).map { "database$it:collection$it" }
        recordCaptor.allValues.forEach {
            it.size shouldBe 100
        }

        recordCaptor.allValues.flatten().forEachIndexed { index, hbasePayload ->
            String(hbasePayload.key) shouldBe index.toString()
            String(hbasePayload.body) shouldBe """{"message":{"_id":{"id":"${(index % 100) + 1}"},"timestamp_created_from":"epoch"}}"""
        }
    }

    private fun verifyKafkaInteractions(consumer: KafkaConsumer<ByteArray, ByteArray>) {
        verifySuccesses(consumer)
        verifyFailures(consumer)
        verifyNoMoreInteractions(consumer)
    }

    private fun verifyFailures(consumer: KafkaConsumer<ByteArray, ByteArray>) {
        val topicPartitionCaptor = argumentCaptor<TopicPartition>()
        val committedCaptor = argumentCaptor<TopicPartition>()
        val positionCaptor = argumentCaptor<Long>()
        verify(consumer, times(5)).committed(committedCaptor.capture())

        committedCaptor.allValues.forEachIndexed { index, topicPartition ->
            val topic = topicPartition.topic()
            val partition = topicPartition.partition()
            val topicNumber = (index * 2 + 1)
            partition shouldBe 10 - topicNumber
            topic shouldBe topicName(topicNumber)
        }

        verify(consumer, times(5)).seek(topicPartitionCaptor.capture(), positionCaptor.capture())

        topicPartitionCaptor.allValues.zip(positionCaptor.allValues).forEachIndexed { index, pair ->
            val topicNumber = index * 2 + 1
            val topicPartition = pair.first
            val position = pair.second
            val topic = topicPartition.topic()
            val partition = topicPartition.partition()
            topic shouldBe topicName(topicNumber)
            partition shouldBe 10 - topicNumber
            position shouldBe topicNumber * 10
        }
    }

    private fun verifySuccesses(consumer: KafkaConsumer<ByteArray, ByteArray>) {
        val commitCaptor = argumentCaptor<Map<TopicPartition, OffsetAndMetadata>>()
        verify(consumer, times(5)).commitSync(commitCaptor.capture())
        commitCaptor.allValues.forEachIndexed { index, element ->
            val topicNumber = (index + 1) * 2
            element.size shouldBe 1
            val topicPartition = TopicPartition(topicName(topicNumber), 10 - topicNumber)
            element[topicPartition] shouldNotBe null
            element[topicPartition]?.offset() shouldBe (topicNumber * 20 * 100) + 1
        }
    }

    private fun consumerRecords()  =
        ConsumerRecords((1..10).associate { topicNumber ->
            TopicPartition(topicName(topicNumber), 10 - topicNumber) to (1..100).map { recordNumber ->
                val body = Bytes.toBytes(json(recordNumber))
                val key = Bytes.toBytes(recordNumber)
                mock<ConsumerRecord<ByteArray, ByteArray>> {
                    on { value() } doReturn body
                    on { key() } doReturn key
                    on { offset() } doReturn (topicNumber * recordNumber * 20).toLong()
                }
            }
        })

    private fun messageParser() =
            mock<MessageParser> {
                val hbaseKeys = (0..1000000).map { Bytes.toBytes("$it") }
                on { generateKeyFromRecordBody(any()) } doReturnConsecutively hbaseKeys
            }

    private fun kafkaConsumer() =
            mock<KafkaConsumer<ByteArray, ByteArray>> {
                repeat(10) { topicNumber ->
                    on {
                        committed(TopicPartition(topicName(topicNumber), 10 - topicNumber))
                    } doReturn OffsetAndMetadata((topicNumber * 10).toLong(), "")
                }
            }

    private fun hbaseClient() =
            mock<HbaseClient> {
                on { putList(any(), any()) } doAnswer {
                    val tableName = it.getArgument<String>(0)
                    val matchResult = Regex("""[13579]$""").find(tableName)
                    if (matchResult != null) {
                        throw IOException("Table: '$tableName'.")
                    }
                }
            }

    private fun json(id: Any) = """{ "message": { "_id": { "id": "$id" } } }"""
    private fun topicName(topicNumber: Int) = "db.database$topicNumber.collection$topicNumber"

}
