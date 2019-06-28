import org.apache.hadoop.hbase.*
import org.apache.hadoop.hbase.client.ConnectionFactory
import org.apache.hadoop.hbase.client.Put
import org.apache.log4j.Logger

class HbaseClient(
    zookeeperHosts: String,
    zookeeperPort: Int,
    private val namespace: String,
    private val family: ByteArray,
    private val column: ByteArray
) {

    private val hbase = ConnectionFactory.createConnection(HBaseConfiguration.create().apply {
        this.set("hbase.zookeeper.quorum", zookeeperHosts)
        this.setInt("hbase.zookeeper.port", zookeeperPort)
    })!!

    private val logger = Logger.getLogger(this.javaClass)!!

    init {
        val allNamespaces = hbase.admin.listNamespaceDescriptors().map { it.name }
        logger.debug(allNamespaces)

        if (namespace !in allNamespaces) {
            logger.info("Creating namespace %s".format(namespace))
            val namespaceDescriptor = NamespaceDescriptor.create(namespace).build()
            hbase.admin.createNamespace(namespaceDescriptor)
        }
    }

    fun createTopicTable(
        topic: ByteArray,
        maxVersions: Int,
        minVersions: Int = 1,
        timeToLive: Int = HConstants.FOREVER
    ) {
        val allTables = hbase.admin.listTableNamesByNamespace(namespace).map { it.qualifier }
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
                    timeToLive
                )
            )
            hbase.admin.createTable(HTableDescriptor(TableName.valueOf(namespace.toByteArray(), topic)).apply {
                this.addFamily(HColumnDescriptor(family).apply {
                    this.minVersions = minVersions
                    this.maxVersions = maxVersions
                    this.timeToLive = timeToLive
                })
            })
        }
    }

    fun putVersion(topic: ByteArray, key: ByteArray, body: ByteArray, version: Long) {
        val table = hbase.getTable(TableName.valueOf(namespace.toByteArray(), topic))
        table.put(Put("my_key".toByteArray()).apply {
            this.addColumn(
                family,
                column,
                version,
                body
            )
        })
    }
}