package lib

import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.client.*
import org.apache.hadoop.hbase.io.TimeRange

class HbaseTestClient {
    private val namespace = "k2hb".toByteArray()
    private val family = "cf".toByteArray()
    private val qualifier = "data".toByteArray()

    private val zookeeperQuorum = "zookeeper"
    private val zookeeperPort = 2181

    private val connection: Connection = ConnectionFactory.createConnection(
        HBaseConfiguration.create().apply {
            set("hbase.zookeeper.quorum", zookeeperQuorum)
            setInt("hbase.zookeeper.property.clientPort", zookeeperPort)
        }
    )

    fun getTable(tableName: ByteArray): Table {
        return connection.getTable(TableName.valueOf(namespace, tableName))
    }

    fun putCell(tableName: ByteArray, key: ByteArray, timestamp: Long, value: ByteArray) {
        val table = getTable(tableName)

        val request = Put(key, timestamp).apply {
            addColumn(family, qualifier, value)
        }

        table.put(request)
    }

    fun getCellAfterTimestamp(tableName: ByteArray, key: ByteArray, timestamp: Long): ByteArray? {
        val table = getTable(tableName)

        val request = Get(key).apply {
            setTimeRange(timestamp, TimeRange.INITIAL_MAX_TIMESTAMP)
        }

        val result = table.get(request)
        return result.getValue(family, qualifier)
    }

    fun getCellBeforeTimestamp(tableName: ByteArray, key: ByteArray, timestamp: Long): ByteArray? {
        val table = getTable(tableName)

        val request = Get(key).apply {
            setTimeRange(0, timestamp)
        }

        val result = table.get(request)
        return result.getValue(family, qualifier)
    }
}
