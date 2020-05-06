import com.beust.klaxon.JsonObject
import com.beust.klaxon.KlaxonException
import com.google.gson.Gson
import com.nhaarman.mockitokotlin2.*
import io.kotlintest.fail
import io.kotlintest.shouldThrow
import io.kotlintest.specs.StringSpec
import io.mockk.every
import io.mockk.mockkObject
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.consumer.OffsetAndMetadata
import org.apache.kafka.clients.producer.Callback
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.clients.producer.RecordMetadata
import org.apache.kafka.common.Metric
import org.apache.kafka.common.MetricName
import org.apache.kafka.common.PartitionInfo
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.record.TimestampType
import org.junit.Assert.*
import java.io.ByteArrayInputStream
import java.io.ObjectInputStream
import java.time.Duration
import java.util.concurrent.Future
import java.util.logging.Logger


class RecordProcessorTest : StringSpec() {

    private lateinit var mockValidator: Validator
    private lateinit var mockConverter: Converter
    private lateinit var mockMessageParser: MessageParser
    private lateinit var hbaseClient: HbaseClient
    private lateinit var logger: Logger
    private lateinit var processor: RecordProcessor
    private val testByteArray: ByteArray = byteArrayOf(0xA1.toByte(), 0xA1.toByte(), 0xA1.toByte(), 0xA1.toByte())

    override fun isInstancePerTest(): Boolean = true

    private fun reset() {
        println("Before every spec/test case")
        mockValidator = mock()
        mockConverter = spy()
        mockMessageParser = mock()
        hbaseClient = mock()
        logger = mock()
        processor = spy(RecordProcessor(mockValidator, mockConverter))
        doNothing().whenever(mockValidator).validate(any())
        doNothing().whenever(processor).sendMessageToDlq(any(), any())
    }

    init {

        "valid record is sent to hbase successfully" {
            reset()
            val messageBody = """{
                "message": {
                   "_id":{"test_key_a":"test_value_a","test_key_b":"test_value_b"},
                   "_lastModifiedDateTime": "2018-12-14T15:01:02.000+0000",
                }
            }"""

            val persistedBody = Gson().fromJson("""{
                "message": {
                   "_id":{"test_key_a":"test_value_a","test_key_b":"test_value_b"},
                   "_lastModifiedDateTime": "2018-12-14T15:01:02.000+0000",
                    "timestamp_created_from":"_lastModifiedDateTime"
                }
            }""", com.google.gson.JsonObject::class.java).toString()

            val record: ConsumerRecord<ByteArray, ByteArray> = ConsumerRecord("db.database.collection", 1, 11, 1544799662000, TimestampType.CREATE_TIME, 1111, 1, 1, testByteArray, messageBody.toByteArray())
            whenever(mockMessageParser.generateKeyFromRecordBody(any())).thenReturn(testByteArray)
            processor.processRecord(record, hbaseClient, mockMessageParser)
            verify(hbaseClient).putVersion("database:collection", testByteArray, persistedBody.toByteArray(), 1544799662000)
        }

        "record value with invalid json is not sent to hbase" {
            reset()
            val messageBody = """{"message":{"_id":{"test_key_a":,"test_key_b":"test_value_b"}}}"""
            val record: ConsumerRecord<ByteArray, ByteArray> = ConsumerRecord("db.database.collection", 1, 11, 111, TimestampType.CREATE_TIME, 1111, 1, 1, testByteArray, messageBody.toByteArray())
            whenever(mockMessageParser.generateKeyFromRecordBody(any())).thenReturn(testByteArray)
            processor.processRecord(record, hbaseClient, mockMessageParser)

            verifyZeroInteractions(hbaseClient)
        }

        "Exception should be thrown when dlq topic is not available and message  is not sent to hbase" {
            reset()
            mockkObject(DlqProducer)
            val obj = object : org.apache.kafka.clients.producer.Producer<ByteArray, ByteArray> {
                override fun partitionsFor(topic: String?): MutableList<PartitionInfo> {
                    throw Exception("")
                }

                override fun flush() {
                }

                override fun abortTransaction() {
                }

                override fun commitTransaction() {
                }

                override fun beginTransaction() {
                }

                override fun initTransactions() {
                }

                override fun sendOffsetsToTransaction(offsets: MutableMap<TopicPartition, OffsetAndMetadata>?, consumerGroupId: String?) {
                }

                override fun send(record: ProducerRecord<ByteArray, ByteArray>?): Future<RecordMetadata> {
                    throw Exception("")
                }

                override fun send(record: ProducerRecord<ByteArray, ByteArray>?, callback: Callback?): Future<RecordMetadata> {
                    throw Exception("")
                }

                override fun close() {
                }

                override fun close(timeout: Duration?) {
                }

                override fun metrics(): MutableMap<MetricName, out Metric> {
                    throw Exception("")
                }
            }
            every { DlqProducer.getInstance() } returns obj
            val processor = RecordProcessor(mockValidator, mockConverter)
            val messageBody = """{"message":{"_id":{"test_key_a":,"test_key_b":"test_value_b"}}}"""
            val record: ConsumerRecord<ByteArray, ByteArray> = ConsumerRecord("db.database.collection", 1, 11, 111, TimestampType.CREATE_TIME, 1111, 1, 1, testByteArray, messageBody.toByteArray())
            whenever(mockMessageParser.generateKeyFromRecordBody(any())).thenReturn(testByteArray)

            shouldThrow<DlqException> {
                processor.processRecord(record, hbaseClient, mockMessageParser)
            }

            verifyZeroInteractions(hbaseClient)
        }

        "record value with invalid _id field is not sent to hbase" {
            reset()
            val messageBody = """{
                "message": {
                   "id":{"test_key_a":"test_value_a","test_key_b":"test_value_b"},
                   "_lastModifiedDateTime": "2018-12-14T15:01:02.000+0000",
                }
            }"""
            val record: ConsumerRecord<ByteArray, ByteArray> = ConsumerRecord("db.database.collection", 1, 11, 1544799662000, TimestampType.CREATE_TIME, 1111, 1, 1, testByteArray, messageBody.toByteArray())
            whenever(mockMessageParser.generateKeyFromRecordBody(any())).thenReturn(ByteArray(0))

            processor.processRecord(record, hbaseClient, mockMessageParser)

            verifyZeroInteractions(hbaseClient)
        }

        "exception in hbase communication causes severe log message" {
            reset()
            val messageBody = """{
                "message": {
                   "_id":{"test_key_a":"test_value_a","test_key_b":"test_value_b"},
                   "_lastModifiedDateTime": "2018-12-14T15:01:02.000+0000",
                }
            }"""
            val persistedBody = Gson().fromJson("""{
                "message": {
                    "_id":{"test_key_a":"test_value_a","test_key_b":"test_value_b"},
                    "_lastModifiedDateTime": "2018-12-14T15:01:02.000+0000",
                    "timestamp_created_from":"_lastModifiedDateTime"
                }
            }""", com.google.gson.JsonObject::class.java).toString()

            val record: ConsumerRecord<ByteArray, ByteArray> = ConsumerRecord("db.database.collection", 1, 11, 1544799662000, TimestampType.CREATE_TIME, 1111, 1, 1, testByteArray, messageBody.toByteArray())
            whenever(mockMessageParser.generateKeyFromRecordBody(any())).thenReturn(testByteArray)
            whenever(hbaseClient.putVersion("database:collection", testByteArray, persistedBody.toByteArray(), 1544799662000)).doThrow(RuntimeException("testException"))

            try {
                processor.processRecord(record, hbaseClient, mockMessageParser)
                fail("test did not throw an exception")
            } catch (e: HbaseReadException) {
                assertEquals("Error writing record to HBase: java.lang.RuntimeException: testException", e.localizedMessage)
            }
        }

        "Malformed record object can be converted to bytearray " {
            reset()
            val malformedRecord = MalformedRecord("key", "junk", "Not a valid json")
            val byteArray = processor.getObjectAsByteArray(malformedRecord)
            val bi = ByteArrayInputStream(byteArray)
            val oi = ObjectInputStream(bi)
            val actual = oi.readObject()
            assertEquals(malformedRecord, actual)
        }

        "Json that fails schema validation is sent to the dlq" {
            reset()
            val messageBody = "Hello everyone"
            val record: ConsumerRecord<ByteArray, ByteArray> = ConsumerRecord("db.database.collection", 1, 11, 1544799662000, TimestampType.CREATE_TIME, 1111, 1, 1, "key".toByteArray(), messageBody.toByteArray())
            whenever(mockMessageParser.generateKeyFromRecordBody(any())).thenReturn(testByteArray)
            val jsonObject = JsonObject()
            doReturn(jsonObject).`when`(mockConverter).convertToJson(record.value())
            doThrow(InvalidMessageException("oops!!", Exception())).`when`(mockValidator).validate(jsonObject.toJsonString())

            processor.processRecord(record, hbaseClient, mockMessageParser)

            verifyZeroInteractions(hbaseClient)
            verify(processor, times(1)).sendMessageToDlq(eq(record), eq("Invalid schema for key:db.database.collection:1:11: oops!!"))
        }

        "Invalid Json that fails parsing is sent to the dlq" {
            reset()
            val messageBody = "Hello everyone"
            val record: ConsumerRecord<ByteArray, ByteArray> = ConsumerRecord("db.database.collection", 1, 11, 1544799662000, TimestampType.CREATE_TIME, 1111, 1, 1, "key".toByteArray(), messageBody.toByteArray())
            doReturn(testByteArray).`when`(mockMessageParser).generateKeyFromRecordBody(any())
            //whenever(mockMessageParser.generateKeyFromRecordBody(any())).thenReturn(testByteArray)
            doThrow(IllegalArgumentException()).`when`(mockConverter).convertToJson(record.value())
            //whenever(mockConverter.convertToJson(record.value())).thenThrow(IllegalArgumentException())

            processor.processRecord(record, hbaseClient, mockMessageParser)

            verifyZeroInteractions(hbaseClient)
            verify(processor, times(1)).sendMessageToDlq(eq(record), eq("Invalid json"))
            verifyZeroInteractions(mockValidator)
        }

        "RecordProcessor throws when hbase connection not available" {
            reset()
            val messageBody = """{
                "message": {
                   "_id":{"test_key_a":"test_value_a","test_key_b":"test_value_b"},
                   "_lastModifiedDateTime": "2018-12-14T15:01:02.000+0000",
                }
            }"""
            val record: ConsumerRecord<ByteArray, ByteArray> = ConsumerRecord(
                "db.database.collection",
                1,
                11,
                1544799662000,
                TimestampType.CREATE_TIME,
                1111,
                1,
                1,
                "key".toByteArray(),
                messageBody.toByteArray()
            )

            whenever(mockMessageParser.generateKeyFromRecordBody(any())).thenReturn(testByteArray)

            val mockConnection = mock<org.apache.hadoop.hbase.client.Connection> {
                on { isClosed } doReturn true
                on { getTable(any()) } doThrow java.io.IOException() 
            }

            val hbaseClientMock = HbaseClient(mockConnection, "cf".toByteArray(), "record".toByteArray())
            shouldThrow<HbaseReadException> {
                processor.processRecord(record, hbaseClientMock, mockMessageParser)
            }
        }

        "valid json is converted and validated and returns json object" {
            reset()
            val messageBody = "Hello everyone"
            val record: ConsumerRecord<ByteArray, ByteArray> = ConsumerRecord("db.database.collection", 1, 11, 1544799662000, TimestampType.CREATE_TIME, 1111, 1, 1, "key".toByteArray(), messageBody.toByteArray())
            val jsonObject = JsonObject()
            doReturn(jsonObject).`when`(mockConverter).convertToJson(record.value())
            doNothing().`when`(mockValidator).validate(jsonObject.toJsonString())

            val response = processor.convertAndValidateJsonRecord(record)

            assertEquals(jsonObject, response)
        }

        "invalid json is not converted and returns null" {
            reset()
            val messageBody = "Hello everyone"
            val record: ConsumerRecord<ByteArray, ByteArray> = ConsumerRecord("db.database.collection", 1, 11, 1544799662000, TimestampType.CREATE_TIME, 1111, 1, 1, "key".toByteArray(), messageBody.toByteArray())
            doThrow(IllegalArgumentException()).`when`(mockConverter).convertToJson(record.value())

            val response = processor.convertAndValidateJsonRecord(record)

            assertNull(response)
        }

        "invalid json is not validated and returns null" {
            reset()
            val messageBody = "Hello everyone"
            val record: ConsumerRecord<ByteArray, ByteArray> = ConsumerRecord("db.database.collection", 1, 11, 1544799662000, TimestampType.CREATE_TIME, 1111, 1, 1, "key".toByteArray(), messageBody.toByteArray())
            val jsonObject = JsonObject()
            doReturn(jsonObject).`when`(mockConverter).convertToJson(record.value())
            doThrow(InvalidMessageException("oops!!", Exception())).`when`(mockValidator).validate(jsonObject.toJsonString())

            val response = processor.convertAndValidateJsonRecord(record)

            assertNull(response)
        }
    }
}
