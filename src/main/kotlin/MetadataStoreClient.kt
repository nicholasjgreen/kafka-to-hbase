
import org.apache.kafka.clients.consumer.ConsumerRecord
import java.sql.Connection
import java.sql.DriverManager
import java.sql.SQLException
import java.sql.Timestamp
import java.util.*
import kotlin.system.measureTimeMillis

open class MetadataStoreClient(private val connection: Connection): AutoCloseable {

    @Synchronized
    open fun recordProcessingAttempt(hbaseId: String, record: ConsumerRecord<ByteArray, ByteArray>, lastUpdated: Long) {
        if (Config.MetadataStore.writeToMetadataStore) {
            val rowsInserted = preparedStatement(hbaseId, lastUpdated, record).executeUpdate()
            logger.info("Recorded processing attempt", "rows_inserted", "$rowsInserted")
        }
    }

    @Synchronized
    @Throws(SQLException::class)
    open fun recordBatch(payloads: List<HbasePayload>) {
        if (Config.MetadataStore.writeToMetadataStore) {
            logger.info("Putting batch into metadata store", "size", "${payloads.size}")
            val timeTaken = measureTimeMillis {
                with(recordProcessingAttemptStatement) {
                    payloads.forEach {
                        setString(1, textUtils.printableKey(it.key))
                        setTimestamp(2, Timestamp(it.version))
                        setString(3, it.record.topic())
                        setInt(4, it.record.partition())
                        setLong(5, it.record.offset())
                        addBatch()
                    }
                    executeBatch()
                }
            }
            logger.info("Put batch into metadata store", "time_taken", "$timeTaken", "size", "${payloads.size}")
        }
        else {
            logger.info("Not putting batch into metadata store", "write_to_metadata_store", "${Config.MetadataStore.writeToMetadataStore}")
        }
    }

    private fun preparedStatement(hbaseId: String, lastUpdated: Long, record: ConsumerRecord<ByteArray, ByteArray>) =
        recordProcessingAttemptStatement.apply {
            setString(1, hbaseId)
            setTimestamp(2, Timestamp(lastUpdated))
            setString(3, record.topic())
            setInt(4, record.partition())
            setLong(5, record.offset())
        }


    private val recordProcessingAttemptStatement by lazy {
        connection.prepareStatement("""
            INSERT INTO ${Config.MetadataStore.metadataStoreTable} (hbase_id, hbase_timestamp, topic_name, kafka_partition, kafka_offset)
            VALUES (?, ?, ?, ?, ?)
        """.trimIndent())
    }

    companion object {
        private val isUsingAWS = Config.MetadataStore.isUsingAWS
        private val secretHelper: SecretHelperInterface =  if (isUsingAWS) AWSSecretHelper() else DummySecretHelper()

        fun connect(): MetadataStoreClient {
            val (url, properties) = connectionProperties()
            return MetadataStoreClient(DriverManager.getConnection(url, properties))
        }

        fun connectionProperties(): Pair<String, Properties> {
            val hostname = Config.MetadataStore.properties["rds.endpoint"]
            val port = Config.MetadataStore.properties["rds.port"]
            val jdbcUrl = "jdbc:mysql://$hostname:$port/${Config.MetadataStore.properties.getProperty("database")}"
            val secretName = Config.MetadataStore.properties.getProperty("rds.password.secret.name")
            val propertiesWithPassword: Properties = Config.MetadataStore.properties.clone() as Properties
            propertiesWithPassword["password"] = secretHelper.getSecret(secretName)
            return Pair(jdbcUrl, propertiesWithPassword)
        }

        val logger: JsonLoggerWrapper = JsonLoggerWrapper.getLogger(MetadataStoreClient::class.toString())
        val textUtils = TextUtils()
    }

    override fun close() = connection.close()
}
