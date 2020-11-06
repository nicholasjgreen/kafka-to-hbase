
import com.google.gson.Gson
import com.google.gson.JsonObject
import com.nhaarman.mockitokotlin2.*
import io.kotest.core.spec.style.StringSpec
import io.kotest.matchers.shouldBe
import io.kotest.matchers.shouldNotBe
import kotlinx.coroutines.runBlocking
import org.apache.hadoop.hbase.util.Bytes
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.consumer.ConsumerRecords
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.consumer.OffsetAndMetadata
import org.apache.kafka.common.TopicPartition
import java.io.IOException
import java.sql.Connection
import java.sql.PreparedStatement


class ListProcessorTest : StringSpec() {

    init {
        "Only commits offsets on success, resets position on failure" {
            val processor = ListProcessor(mock(), Converter())
            val hbaseClient = hbaseClient()
            val metadataStoreClient = metadataStoreClient()
            val consumer = kafkaConsumer()
            val s3Service = archiveAwsS3Service()
            val manifestService = manifestAwsS3Service()
            processor.processRecords(hbaseClient, consumer, metadataStoreClient, s3Service, manifestService, messageParser(), consumerRecords())
            verifyS3Interactions(s3Service)
            verifyHbaseInteractions(hbaseClient)
            verifyKafkaInteractions(consumer)
            verifyMetadataStoreInteractions(metadataStoreClient)
        }
    }

    private fun verifyMetadataStoreInteractions(metadataStoreClient: MetadataStoreClient) {
        val captor = argumentCaptor<List<HbasePayload>>()
        verify(metadataStoreClient, times(10)).recordBatch(captor.capture())
        validateMetadataHbasePayloads(captor)
    }

    private fun verifyS3Interactions(s3Service: ArchiveAwsS3Service) = runBlocking {
        val tableCaptor = argumentCaptor<String>()
        val payloadCaptor = argumentCaptor<List<HbasePayload>>()
        verify(s3Service, times(10)).putObjects(tableCaptor.capture(), payloadCaptor.capture())
        validateTableNames(tableCaptor)
        validateHbasePayloads(payloadCaptor)
    }


    private fun verifyHbaseInteractions(hbaseClient: HbaseClient) {
        verifyHBasePuts(hbaseClient)
        verifyNoMoreInteractions(hbaseClient)
    }

    private fun verifyHBasePuts(hbaseClient: HbaseClient) = runBlocking {
        val tableNameCaptor = argumentCaptor<String>()
        val recordCaptor = argumentCaptor<List<HbasePayload>>()
        verify(hbaseClient, times(10)).putList(tableNameCaptor.capture(), recordCaptor.capture())
        validateTableNames(tableNameCaptor)
        validateHbasePayloads(recordCaptor)
    }

    private fun validateHbasePayloads(captor: KArgumentCaptor<List<HbasePayload>>) {
        captor.allValues.size shouldBe 10
        captor.allValues.forEachIndexed { payloadsNo, payloads ->
            payloads.size shouldBe 100
            payloads.forEachIndexed { index, payload ->
                String(payload.key).toInt() shouldBe index + ((payloadsNo) * 100)
                val body = Gson().fromJson(String(payload.body), JsonObject::class.java)
                val putTime = body["put_time"].asJsonPrimitive.asString
                putTime shouldNotBe null
                String(payload.body) shouldBe hbaseBody(index, putTime)
                payload.record.partition() shouldBe (index + 1) % 20
                payload.record.offset() shouldBe ((payloadsNo + 1) * (index + 1)) * 20
            }
        }
    }

    private fun validateMetadataHbasePayloads(captor: KArgumentCaptor<List<HbasePayload>>) {
        captor.allValues.size shouldBe 10
        captor.allValues.forEachIndexed { payloadsNo, payloads ->
            payloads.size shouldBe 100
            payloads.forEachIndexed { index, payload ->
                String(payload.key).toInt() shouldBe (index) + (payloadsNo * 100)
                val body = Gson().fromJson(String(payload.body), JsonObject::class.java)
                val putTime = body["put_time"].asJsonPrimitive.asString
                putTime shouldNotBe null
                String(payload.body) shouldBe hbaseBody(index, putTime)
                payload.record.partition() shouldBe (index + 1) % 20
                payload.record.offset() shouldBe ((payloadsNo + 1) * (index + 1)) * 20
            }
        }
    }

    private fun validateTableNames(tableCaptor: KArgumentCaptor<String>) {
        tableCaptor.allValues.forEachIndexed { index, tableName ->
            tableName shouldBe tableName(index + 1)
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

    private fun messageParser() =
            mock<MessageParser> {
                val hbaseKeys = (0..1000000).map { Pair("id", Bytes.toBytes("$it")) }
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

    private fun metadataStoreClient(): MetadataStoreClient {
        val statement = mock<PreparedStatement>()
        val connection = mock<Connection> {
            on {prepareStatement(any())} doReturn statement
        }
        return spy(MetadataStoreClient(connection))
    }

    private fun consumerRecords()  =
            ConsumerRecords((1..10).associate { topicNumber ->
                TopicPartition(topicName(topicNumber), 10 - topicNumber) to (1..100).map { recordNumber ->
                    val body = Bytes.toBytes(json(recordNumber))
                    val key = Bytes.toBytes("${topicNumber + recordNumber}")
                    val offset = (topicNumber * recordNumber * 20).toLong()
                    mock<ConsumerRecord<ByteArray, ByteArray>> {
                        on { value() } doReturn body
                        on { key() } doReturn key
                        on { offset() } doReturn offset
                        on { partition() } doReturn recordNumber % 20
                    }
                }
            })

    private fun archiveAwsS3Service(): ArchiveAwsS3Service = mock { on { runBlocking { putObjects(any(), any()) } } doAnswer { } }
    private fun manifestAwsS3Service(): ManifestAwsS3Service = mock { on { runBlocking { putManifestFile(any()) } } doAnswer { } }
    
    private fun json(id: Any) = """{ "message": { "_id": { "id": "$id" } } }"""
    private fun topicName(topicNumber: Int) = "db.database%02d.collection%02d".format(topicNumber, topicNumber)
    private fun hbaseBody(index: Int, putTime: String = "") =
            """{"message":{"_id":{"id":"${(index % 100) + 1}"},"timestamp_created_from":"epoch"}""" + (if (putTime.isNotBlank()) ""","put_time":"$putTime"""" else "") + "}"
    private fun tableName(tableNumber: Int) =  "database%02d:collection%02d".format(tableNumber, tableNumber)
}
