import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.client.*
import org.apache.hadoop.hbase.io.TimeRange
import org.apache.hadoop.hbase.util.Bytes
import java.security.Security

open class HbaseClient(
    val connection: Connection,
    val dataTable: String,
    val dataFamily: ByteArray,
    val topicTable: String,
    val topicFamily: ByteArray,
    val topicQualifier: ByteArray
) {

    init {
        Security.setProperty("networkaddress.cache.ttl", (getEnv("K2HB_DNS_TTL") ?: "60"))
    }
    
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

    @Throws(java.io.IOException::class)
    open fun putVersion(topic: ByteArray, key: ByteArray, body: ByteArray, version: Long) {
        if(connection.isClosed) throw java.io.IOException("HBase connection is closed")

        connection.getTable(TableName.valueOf(dataTable)).use { table ->
            table.put(Put(key).apply {
                this.addColumn(
                    dataFamily,
                    topic,
                    version,
                    body
                )
            })
        }

        connection.getTable(TableName.valueOf(topicTable)).use { table ->
            table.increment(Increment(topic).apply {
                addColumn(
                    topicFamily,
                    topicQualifier,
                    1
                )
            })
        }
    }

    fun getCellAfterTimestamp(topic: ByteArray, key: ByteArray, timestamp: Long): ByteArray? {
        connection.getTable(TableName.valueOf(dataTable)).use { table ->
            val result = table.get(Get(key).apply {
                setTimeRange(timestamp, TimeRange.INITIAL_MAX_TIMESTAMP)
            })

            return result.getValue(dataFamily, topic)
        }

    }

    fun getCellBeforeTimestamp(topic: ByteArray, key: ByteArray, timestamp: Long): ByteArray? {
        connection.getTable(TableName.valueOf(dataTable)).use { table ->
            val result = table.get(Get(key).apply {
                setTimeRange(0, timestamp)
            })

            return result.getValue(dataFamily, topic)
        }
    }

    fun getCount(key: ByteArray): Long {
        connection.getTable(TableName.valueOf(topicTable)).use { table ->
            val result = table.get(Get(key).apply {
                addColumn(topicFamily, topicQualifier)
            })

            val bytes = result?.getValue(topicFamily, topicQualifier) ?: ByteArray(8)
            return Bytes.toLong(bytes)
        }
    }

    fun close() = connection.close()
}
