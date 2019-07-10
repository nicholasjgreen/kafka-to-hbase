import org.apache.hadoop.hbase.*
import org.apache.hadoop.hbase.client.Connection
import org.apache.hadoop.hbase.client.ConnectionFactory
import org.apache.hadoop.hbase.client.Put
import org.apache.log4j.Logger
import java.time.Duration

class HbaseClient(
    private val connection: Connection,
    private val namespace: String,
    private val family: ByteArray,
    private val column: ByteArray
) {
    private val logger = Logger.getLogger(this.javaClass)!!

    init {
        val allNamespaces = connection.admin.listNamespaceDescriptors().map { it.name }
        logger.debug(allNamespaces)

        if (namespace !in allNamespaces) {
            logger.info("Creating namespace %s".format(namespace))
            val namespaceDescriptor = NamespaceDescriptor.create(namespace).build()
            connection.admin.createNamespace(namespaceDescriptor)
        }
    }

    companion object {
        fun connect() = HbaseClient(
            ConnectionFactory.createConnection(HBaseConfiguration.create(Config.Hbase.config)),
            Config.Hbase.namespace,
            Config.Hbase.family.toByteArray(),
            Config.Hbase.qualifier.toByteArray()
        )
    }

    fun createTopicTable(
        topic: ByteArray,
        maxVersions: Int,
        minVersions: Int = 1,
        timeToLive: Duration? = null
    ) {
        val allTables = connection.admin.listTableNamesByNamespace(namespace).map { it.qualifier }
        logger.debug(allTables)

        if (!allTables.any {
                // ByteArray does not compare contents when using == so .contentEquals must be used explicitly
                @Suppress("ReplaceCallWithBinaryOperator")
                it contentEquals topic
            }) {
            logger.info(
                "Creating table %s:%s with family %s with max versions %d, min versions %s and TTL %d".format(
                    namespace,
                    String(topic),
                    String(family),
                    maxVersions,
                    minVersions,
                    timeToLive?.seconds ?: 0
                )
            )

            // Can't use toSeconds because in Java8 it was shadows by BigDecimal.toSeconds, which is private
            val timeToLiveSeconds = timeToLive?.run { toMillis().toInt() / 1000 } ?: HConstants.FOREVER

            connection.admin.createTable(HTableDescriptor(TableName.valueOf(namespace.toByteArray(), topic)).apply {
                this.addFamily(HColumnDescriptor(family).apply {
                    this.minVersions = minVersions
                    this.maxVersions = maxVersions
                    this.timeToLive = timeToLiveSeconds
                })
            })
        }
    }

    fun putVersion(topic: ByteArray, key: ByteArray, body: ByteArray, version: Long) {
        val table = connection.getTable(TableName.valueOf(namespace.toByteArray(), topic))
        table.put(Put(key).apply {
            this.addColumn(
                family,
                column,
                version,
                body
            )
        })
    }

    fun close() = connection.close()
}