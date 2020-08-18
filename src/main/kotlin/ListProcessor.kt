import com.beust.klaxon.JsonObject
import org.apache.hadoop.hbase.util.Bytes
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.consumer.ConsumerRecords
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.consumer.OffsetAndMetadata
import org.apache.kafka.common.TopicPartition
import java.io.IOException

class ListProcessor(validator: Validator, private val converter: Converter): BaseProcessor(validator, converter) {

    fun processRecords(hbase: HbaseClient, consumer: KafkaConsumer<ByteArray, ByteArray>,
                       parser: MessageParser,
                       records: ConsumerRecords<ByteArray, ByteArray>) {
        records.partitions().forEach { partition ->
            val partitionRecords = records.records(partition)
            val payloads = payloads(partitionRecords, parser)
            textUtils.qualifiedTableName(partition.topic())?.let { table ->
                try {
                    hbase.putList(table, payloads)
                    val lastPosition = lastPosition(partitionRecords)
                    logger.info("Batch succeeded, committing offset", "topic", partition.topic(), "partition",
                            "${partition.partition()}", "offset", "$lastPosition")
                    consumer.commitSync(mapOf(partition to OffsetAndMetadata(lastPosition + 1)))
                    logSuccessfulPuts(table, payloads)
                } catch (e: IOException) {
                    val lastCommittedOffset = lastCommittedOffset(consumer, partition)
                    consumer.seek(partition, lastCommittedOffset)
                    logger.error("Batch failed, not committing offset, resetting position to last commit", e,
                            "error", e.message ?: "No message", "topic", partition.topic(),
                            "partition", "${partition.partition()}", "committed_offset", "$lastCommittedOffset")
                    logFailedPuts(table, payloads)
                }
            }
        }
    }

    private fun logFailedPuts(table: String, payloads: List<HbasePayload>) =
            payloads.forEach {
                logger.error("Failed to put record", "table", table,
                        "key", textUtils.printableKey(it.key), "version", "${it.version}")
            }


    private fun logSuccessfulPuts(table: String, payloads: List<HbasePayload>) =
            payloads.forEach {
                logger.info("Put record", "table", table, "key", textUtils.printableKey(it.key), "version", "${it.version}")
            }


    private fun lastCommittedOffset(consumer: KafkaConsumer<ByteArray, ByteArray>, partition: TopicPartition): Long =
            consumer.committed(partition).offset()

    private fun lastPosition(partitionRecords: MutableList<ConsumerRecord<ByteArray, ByteArray>>) =
            partitionRecords[partitionRecords.size - 1].offset()

    private fun payloads(records: List<ConsumerRecord<ByteArray, ByteArray>>, parser: MessageParser): List<HbasePayload> =
            records.mapNotNull { record ->
                recordAsJson(record)?.let { json ->
                    val formattedKey = parser.generateKeyFromRecordBody(json)
                    if (formattedKey.isNotEmpty()) hbasePayload(json, formattedKey) else null
                }
            }

    private fun hbasePayload(json: JsonObject, formattedKey: ByteArray): HbasePayload {
        val (timestamp, source) = converter.getLastModifiedTimestamp(json)
        val message = json["message"] as JsonObject
        message["timestamp_created_from"] = source
        val version = converter.getTimestampAsLong(timestamp)
        return HbasePayload(formattedKey, Bytes.toBytes(json.toJsonString()), version)
    }


    companion object {
        private val textUtils = TextUtils()
        private val logger: JsonLoggerWrapper = JsonLoggerWrapper.getLogger(ListProcessor::class.toString())
    }

}
