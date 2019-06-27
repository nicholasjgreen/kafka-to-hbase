import org.apache.hadoop.hbase.*
import org.apache.hadoop.hbase.client.ConnectionFactory
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.client.Scan
import org.apache.log4j.ConsoleAppender
import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.log4j.PatternLayout

fun main() {
    val consoleAppender = ConsoleAppender()
    consoleAppender.layout = PatternLayout("%d [%p|%c|%C{1}] %m%n")
    consoleAppender.threshold = Level.INFO
    consoleAppender.activateOptions()
    Logger.getRootLogger().addAppender(consoleAppender)

    val logger = Logger.getLogger("testing-hbase")

    val conf = HBaseConfiguration.create()
    conf.set("hbase.zookeeper.quorum", "hbase")

    val hbase = ConnectionFactory.createConnection(conf)

    val namespaceName = "k2hb"
    val allNamespaces = hbase.admin.listNamespaceDescriptors().map { it.name }
    logger.debug(allNamespaces)

    if (namespaceName !in allNamespaces) {
        logger.info("Creating namespace %s".format(namespaceName))
        val namespaceDescriptor = NamespaceDescriptor.create(namespaceName).build()
        hbase.admin.createNamespace(namespaceDescriptor)
    }

    val tableName = "k2hb:testtable"
    val allTables = hbase.admin.listTableNamesByNamespace(namespaceName).map { it.nameAsString }
    logger.debug(allTables)

    if (tableName !in allTables) {
        logger.info("Creating table %s".format(tableName))
        hbase.admin.createTable(HTableDescriptor(TableName.valueOf(tableName)).apply {
            this.addFamily(HColumnDescriptor("cf").apply {
                this.minVersions = 1
                this.maxVersions = 10
            })
        })
    }

    logger.info("Writing two versions")
    val table = hbase.getTable(TableName.valueOf(tableName))
    table.put(Put("my_key".toByteArray()).apply {
        this.addColumn(
            "cf".toByteArray(),
            "my_column".toByteArray(),
            20190626141700,
            "my_value_old".toByteArray()
        )
        this.addColumn(
            "cf".toByteArray(),
            "my_column".toByteArray(),
            20190626151200,
            "my_value_latest".toByteArray()
        )
    })

    logger.info("Scanning for latest version...")
    val latestScanner = table.getScanner(Scan().apply {
        this.addColumn("cf".toByteArray(), "my_column".toByteArray())
    })

    logger.info("Scanning done")
    for (result in latestScanner) {
        for (version in result.map.flatMap { familyPair ->
            familyPair.value.flatMap { qualifierPair ->
                qualifierPair.value.map { timestampPair ->
                    "%s:%s @ %d => %s".format(
                        String(familyPair.key),
                        String(qualifierPair.key),
                        timestampPair.key,
                        String(timestampPair.value)
                    )
                }
            }
        }) {

            logger.info(version)
        }
    }

    logger.info("Scanning for older version...")
    val olderScanner = table.getScanner(Scan().apply {
        this.setTimeRange(0, 20190626141800)
        this.addColumn("cf".toByteArray(), "my_column".toByteArray())
    })

    logger.info("Scanning done")
    for (result in olderScanner) {
        for (version in result.map.flatMap { familyPair ->
            familyPair.value.flatMap { qualifierPair ->
                qualifierPair.value.map { timestampPair ->
                    "%s:%s @ %d => %s".format(
                        String(familyPair.key),
                        String(qualifierPair.key),
                        timestampPair.key,
                        String(timestampPair.value)
                    )
                }
            }
        }) {

            logger.info(version)
        }
    }
}