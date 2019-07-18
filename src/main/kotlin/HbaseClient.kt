import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.client.*
import org.apache.hadoop.hbase.io.TimeRange
import org.apache.hadoop.hbase.util.Bytes

class HbaseClient(
    private val connection: Connection,
    private val dataTable: String,
    private val dataFamily: ByteArray,
    private val topicTable: String,
    private val topicFamily: ByteArray,
    private val topicQualifier: ByteArray
) {
    companion object {
        fun connect() = HbaseClient(
            ConnectionFactory.createConnection(HBaseConfiguration.create(Config.Hbase.config)),
            Config.Hbase.dataTable,
            Config.Hbase.dataFamily.toByteArray(),
            Config.Hbase.topicTable,
            Config.Hbase.topicFamily.toByteArray(),
            Config.Hbase.topicQualifier.toByteArray()
        )
    }

    fun putVersion(topic: ByteArray, key: ByteArray, body: ByteArray, version: Long) {
        val dataTable = connection.getTable(TableName.valueOf(dataTable))
        val topicTable = connection.getTable(TableName.valueOf(topicTable))

        dataTable.put(Put(key).apply {
            this.addColumn(
                dataFamily,
                topic,
                version,
                body
            )
        })

        topicTable.increment(Increment(topic).apply {
            addColumn(
                topicFamily,
                topicQualifier,
                1
            )
        })
    }

     fun getCellAfterTimestamp(topic: ByteArray, key: ByteArray, timestamp: Long): ByteArray? {
        val table = connection.getTable(TableName.valueOf(dataTable))
        val result = table.get(Get(key).apply {
            setTimeRange(timestamp, TimeRange.INITIAL_MAX_TIMESTAMP)
        })

        return result.getValue(dataFamily, topic)
    }

    fun getCellBeforeTimestamp(topic: ByteArray, key: ByteArray, timestamp: Long): ByteArray? {
        val table = connection.getTable(TableName.valueOf(dataTable))
        val result = table.get(Get(key).apply {
            setTimeRange(0, timestamp)
        })

        return result.getValue(dataFamily, topic)
    }

    fun getCount(key: ByteArray): Long {
        val table = connection.getTable(TableName.valueOf(topicTable))
        val result = table.get(Get(key).apply {
            addColumn(topicFamily, topicQualifier)
        })

        val bytes = result?.getValue(topicFamily, topicQualifier) ?: ByteArray(8)
        return Bytes.toLong(bytes)
    }

    fun close() = connection.close()
}