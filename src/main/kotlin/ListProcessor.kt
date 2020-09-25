import com.beust.klaxon.JsonObject
import kotlinx.coroutines.*
import org.apache.hadoop.hbase.util.Bytes
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.consumer.ConsumerRecords
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.consumer.OffsetAndMetadata
import org.apache.kafka.common.TopicPartition

class ListProcessor(validator: Validator, private val converter: Converter) : BaseProcessor(validator, converter) {

    fun processRecords(hbase: HbaseClient, consumer: KafkaConsumer<ByteArray, ByteArray>, metadataClient: MetadataStoreClient,
        s3Service: ArchiveAwsS3Service, manifestService: ManifestAwsS3Service, parser: MessageParser, records: ConsumerRecords<ByteArray, ByteArray>) {
        runBlocking {
            records.partitions().forEach { partition ->
                val partitionRecords = records.records(partition)
                val payloads = payloads(partitionRecords, parser)
                textUtils.qualifiedTableName(partition.topic())?.let { table ->
                    coroutineScope {
                        val s3OkDeferred = async { putInS3(s3Service, table, payloads) }
                        val hbaseOk = putInHbase(hbase, table, payloads)

                        val commitOffset = if (hbaseOk) {
                            putInMetadataStore(metadataClient, payloads)
                        }
                        else false

                        if (s3OkDeferred.await() && commitOffset) {
                            val lastPosition = lastPosition(partitionRecords)
                            logger.info("Batch succeeded, committing offset", "topic", partition.topic(),
                                "partition","${partition.partition()}", "offset", "$lastPosition")
                            consumer.commitSync(mapOf(partition to OffsetAndMetadata(lastPosition + 1)))
                            logSuccessfulPuts(table, payloads)
                            if (Config.ManifestS3.writeManifests) {
                                putManifest(manifestService, table, payloads)
                            }
                        } else {
                            lastCommittedOffset(consumer, partition)?.let { consumer.seek(partition, it) }
                            logger.error("Batch failed, not committing offset, resetting position to last commit",
                                "topic", partition.topic(), "partition", "${partition.partition()}")
                            logFailedPuts(table, payloads)
                        }
                    }
                }
            }
        }
    }

    private suspend fun putInMetadataStore(metadataClient: MetadataStoreClient, payloads: List<HbasePayload>) =
        withContext(Dispatchers.IO) {
            try {
                metadataClient.recordBatch(payloads)
                true
            } catch (e: Exception) {
                logger.error("Failed to put batch into metadatastore", e, "error", e.message ?: "")
                false
            }
        }

    private suspend fun putInS3(s3Service: ArchiveAwsS3Service, table: String, payloads: List<HbasePayload>) =
        withContext(Dispatchers.IO) {
            try {
                if (Config.ArchiveS3.batchPuts) {
                    s3Service.putObjectsAsBatch(table, payloads)
                } else {
                    s3Service.putObjects(table, payloads)
                }
                true
            } catch (e: Exception) {
                e.printStackTrace()
                logger.error("Failed to put batch into s3", e, "error", e.message ?: "")
                false
            }
        }

    private suspend fun putInHbase(hbase: HbaseClient, table: String, payloads: List<HbasePayload>) =
        withContext(Dispatchers.IO) {
            try {
                hbase.putList(table, payloads)
                true
            } catch (e: Exception) {
                logger.error("Failed to put batch into hbase", e, "error", e.message ?: "")
                false
            }
        }


    private fun logFailedPuts(table: String, payloads: List<HbasePayload>) =
        payloads.forEach {
            logger.error(
                "Failed to put record", "table", table,
                "key", textUtils.printableKey(it.key), "version", "${it.version}"
            )
        }

    private suspend fun putManifest(manifestService: ManifestAwsS3Service, table: String, payloads: List<HbasePayload>) =
        withContext(Dispatchers.IO) {
            try {
                manifestService.putManifestFile(table, payloads)
                true
            } catch (e: Exception) {
                e.printStackTrace()
                logger.error("Failed to put manifest file in to s3", e, "error", e.message ?: "")
                false
            }
        }


    private fun logSuccessfulPuts(table: String, payloads: List<HbasePayload>) =
        payloads.forEach {
            logger.info("Put record", "table", table, "key", textUtils.printableKey(it.key), "version", "${it.version}")
        }


    private fun lastCommittedOffset(consumer: KafkaConsumer<ByteArray, ByteArray>, partition: TopicPartition): Long? =
        consumer.committed(partition)?.let { it.offset() }

    private fun lastPosition(partitionRecords: MutableList<ConsumerRecord<ByteArray, ByteArray>>) =
        partitionRecords[partitionRecords.size - 1].offset()

    private fun payloads(
        records: List<ConsumerRecord<ByteArray, ByteArray>>,
        parser: MessageParser
    ): List<HbasePayload> =
        records.mapNotNull { record ->
            recordAsJson(record)?.let { json ->
                val (unformattedId, formattedKey) = parser.generateKeyFromRecordBody(json)
                val qualifiedId = if (unformattedId == null) "" else unformattedId
                if (formattedKey.isNotEmpty()) hbasePayload(json, qualifiedId, formattedKey, record) else null
            }
        }

    private fun hbasePayload(
        json: JsonObject,
        unformattedId: String,
        formattedKey: ByteArray,
        record: ConsumerRecord<ByteArray, ByteArray>
    ): HbasePayload {
        val (timestamp, source) = converter.getLastModifiedTimestamp(json)
        val message = json["message"] as JsonObject
        message["timestamp_created_from"] = source
        val version = converter.getTimestampAsLong(timestamp)
        return HbasePayload(formattedKey, Bytes.toBytes(json.toJsonString()), unformattedId, version, record)
    }

    companion object {
        private val textUtils = TextUtils()
        private val logger: JsonLoggerWrapper = JsonLoggerWrapper.getLogger(ListProcessor::class.toString())
    }
}
