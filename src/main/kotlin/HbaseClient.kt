import org.apache.hadoop.hbase.*
import org.apache.hadoop.hbase.client.Connection
import org.apache.hadoop.hbase.client.ConnectionFactory
import org.apache.hadoop.hbase.client.Get
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.io.TimeRange

open class HbaseClient(val connection: Connection, private val columnFamily: ByteArray, private val columnQualifier: ByteArray) {

    companion object {
        fun connect() = HbaseClient(
            ConnectionFactory.createConnection(HBaseConfiguration.create(Config.Hbase.config)),
            Config.Hbase.columnFamily.toByteArray(),
            Config.Hbase.columnQualifier.toByteArray())

        val logger: JsonLoggerWrapper = JsonLoggerWrapper.getLogger(HbaseClient::class.toString())
    }

    fun put(table: String, key: ByteArray, body: ByteArray, version: Long) {

        var success = false
        var attempts = 0
        var exception: Exception? = null
        while (!success && attempts < Config.Hbase.retryMaxAttempts) {
            try {
                putVersion(table, key, body, version)
                success = true
            }
            catch (e: Exception) {
                val delay = if (attempts == 0) Config.Hbase.retryInitialBackoff
                else (Config.Hbase.retryInitialBackoff * attempts * Config.Hbase.retryBackoffMultiplier.toFloat()).toLong()
                logger.warn("Failed to put batch ${e.message}", "attempt_number", "${attempts + 1}",
                    "max_attempts", "${Config.Hbase.retryMaxAttempts}", "retry_delay", "$delay", "error_message", "${e.message}")
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
            throw java.io.IOException("HBase connection is closed")
        }

        ensureTable(tableName)

        connection.getTable(TableName.valueOf(tableName)).use { table ->
            table.put(Put(key).apply {
                this.addColumn(columnFamily, columnQualifier, version, body)
            })
        }
    }

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
    fun ensureTable(tableName: String) {
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
                            })
                })
            } catch (e: TableExistsException) {
                logger.info("Didn't create table, table already exists, probably created by another process",
                    "table_name", tableName)
            }
            finally {
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

    fun close() = connection.close()
}
