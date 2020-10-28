
import org.apache.hadoop.hbase.*
import org.apache.hadoop.hbase.client.*
import org.apache.hadoop.hbase.io.TimeRange
import org.apache.hadoop.hbase.io.compress.Compression.Algorithm
import java.io.IOException
import kotlin.system.measureTimeMillis

open class HbaseClient(val connection: Connection, private val columnFamily: ByteArray,
                       private val columnQualifier: ByteArray, private val hbaseRegionReplication: Int): AutoCloseable {

    @Throws(IOException::class)
    open fun putList(tableName: String, payloads: List<HbasePayload>) {
        if (payloads.isNotEmpty()) {
            val timeTaken = measureTimeMillis {
                logger.info("Putting batch into table", "size", "${payloads.size}", "table", tableName)
                ensureTable(tableName)
                connection.getTable(TableName.valueOf(tableName)).use { table ->
                    table.put(payloads.map { payload ->
                        Put(payload.key).apply {
                            addColumn(columnFamily, columnQualifier, payload.version, payload.body)
                        }
                    })
                }
            }
            logger.info("Put batch into table", "time_taken", "$timeTaken", "size", "${payloads.size}", "table", tableName)
        }
    }

    fun put(table: String, key: ByteArray, body: ByteArray, version: Long) {

        var success = false
        var attempts = 0
        var exception: Exception? = null
        while (!success && attempts < Config.Hbase.retryMaxAttempts) {
            try {
                putVersion(table, key, body, version)
                success = true
            } catch (e: Exception) {
                val delay = if (attempts == 0) Config.Hbase.retryInitialBackoff
                else (Config.Hbase.retryInitialBackoff * attempts * Config.Hbase.retryBackoffMultiplier.toFloat()).toLong()
                logger.warn("Failed to put batch ${e.message}", "attempt_number", "${attempts + 1}",
                    "max_attempts", "${Config.Hbase.retryMaxAttempts}", "retry_delay", "$delay", "error_message","${e.message}")
                Thread.sleep(delay)
                exception = e
            }
            finally {
                attempts++
            }
        }

        if (!success) {
            if (exception != null) {
                throw exception
            }
        }

    }

    @Throws(Exception::class)
    open fun putVersion(tableName: String, key: ByteArray, body: ByteArray, version: Long) {

        if (connection.isClosed) {
            throw IOException("HBase connection is closed")
        }

        ensureTable(tableName)

        val printableKey = textUtils.printableKey(key)

        if (Config.Hbase.logKeys) {
            logger.info("Putting record", "key", printableKey, "table", tableName, "version", "$version")
        }

        attemptPut(tableName, key, version, body)

        if (Config.Hbase.logKeys) {
            logger.info("Put record", "key", printableKey, "table", tableName, "version", "$version")
        }
    }


    private fun attemptPut(tableName: String, key: ByteArray, version: Long, body: ByteArray) {

        connection.getTable(TableName.valueOf(tableName)).use { table ->
            if (Config.Hbase.checkExistence) {
                var exists = false
                var attempts = 0
                while (!exists) {
                    putRecord(table, key, version, body)

                    exists = table.exists(Get(key).apply {
                        setTimeStamp(version)
                    })

                    if (!exists) {
                        logger.warn("Put record does not exist","attempts", "$attempts",
                            "key", textUtils.printableKey(key), "table", tableName, "version", "$version")
                        if (++attempts >= Config.Hbase.maxExistenceChecks) {
                            logger.error("Put record does not exist after max retry attempts",
                                "attempts", "$attempts", "key", textUtils.printableKey(key), "table", tableName, "version", "$version")
                            throw Exception("Put record does not exist after max retry attempts: $tableName/${textUtils.printableKey(key)}/$version")
                        }
                    }
                }
            } else {
                putRecord(table, key, version, body)
            }
        }
    }

    private fun putRecord(table: Table, key: ByteArray, version: Long, body: ByteArray) =
        table.put(Put(key).apply {
            addColumn(columnFamily, columnQualifier, version, body)
        })


    fun getCellAfterTimestamp(tableName: String, key: ByteArray, timestamp: Long): ByteArray? {
        connection.getTable(TableName.valueOf(tableName)).use { table ->
            val result = table.get(Get(key).apply {
                setTimeRange(timestamp, TimeRange.INITIAL_MAX_TIMESTAMP)
            })
            return result.getValue(columnFamily, columnQualifier)
        }
    }

    fun getCellBeforeTimestamp(tableName: String, key: ByteArray, timestamp: Long): ByteArray? {
        connection.getTable(TableName.valueOf(tableName)).use { table ->
            val result = table.get(Get(key).apply {
                setTimeRange(0, timestamp)
            })

            return result.getValue(columnFamily, columnQualifier)
        }
    }

    @Synchronized
    open fun ensureTable(tableName: String) {
        val dataTableName = TableName.valueOf(tableName)
        val namespace = dataTableName.namespaceAsString

        if (!namespaces.contains(namespace)) {
            try {
                logger.info("Creating namespace", "namespace", namespace)
                connection.admin.createNamespace(NamespaceDescriptor.create(namespace).build())
            }
            catch (e: NamespaceExistException) {
                logger.info("Namespace already exists, probably created by another process", "namespace", namespace)
            }
            finally {
                namespaces[namespace] = true
            }
        }

        if (!tables.contains(tableName)) {
            logger.info("Creating table", "table_name", "$dataTableName")
            try {
                connection.admin.createTable(HTableDescriptor(dataTableName).apply {
                    addFamily(
                        HColumnDescriptor(columnFamily)
                            .apply {
                                maxVersions = Int.MAX_VALUE
                                minVersions = 1
                                compressionType = Algorithm.GZ
                                compactionCompressionType = Algorithm.GZ
                            })
                    regionReplication = hbaseRegionReplication
                })
            } catch (e: TableExistsException) {
                logger.info("Didn't create table, table already exists, probably created by another process",
                    "table_name", tableName)
            } finally {
                tables[tableName] = true
            }
        }
    }

    private val namespaces by lazy {
        val extantNamespaces = mutableMapOf<String, Boolean>()

        connection.admin.listNamespaceDescriptors()
            .forEach {
                extantNamespaces[it.name] = true
            }

        extantNamespaces
    }

    private val tables by lazy {
        val names = mutableMapOf<String, Boolean>()

        connection.admin.listTableNames().forEach {
            names[it.nameAsString] = true
        }

        names
    }

    companion object {
        fun connect(): HbaseClient {
            logger.info("Hbase connection configuration",
                    HConstants.ZOOKEEPER_ZNODE_PARENT, Config.Hbase.config.get(HConstants.ZOOKEEPER_ZNODE_PARENT),
                    HConstants.ZOOKEEPER_QUORUM, Config.Hbase.config.get(HConstants.ZOOKEEPER_QUORUM),
                    "hbase.zookeeper.port", Config.Hbase.config.get("hbase.zookeeper.port"))

            return HbaseClient(
                    ConnectionFactory.createConnection(HBaseConfiguration.create(Config.Hbase.config)),
                    Config.Hbase.columnFamily.toByteArray(),
                    Config.Hbase.columnQualifier.toByteArray(),
                    Config.Hbase.regionReplication)
        }

        private val logger: JsonLoggerWrapper = JsonLoggerWrapper.getLogger(HbaseClient::class.toString())
        private val textUtils = TextUtils()
    }


    override fun close() = connection.close()

}
