import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.client.Connection
import org.apache.hadoop.hbase.client.ConnectionFactory
import org.apache.hadoop.hbase.client.Get
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.client.Table
import org.apache.hadoop.hbase.io.TimeRange

class HbaseTestClient {
    private def namespace = "k2hb".getBytes()
    private def family = "cf".getBytes()
    private def qualifier = "data".getBytes()

    private def zookeeperQuorum = "zookeeper"
    private def zookeeperPort = 2181

    private Connection connection

    HbaseTestClient() {
        def configuration = HBaseConfiguration.create()
        configuration.with {
            set "hbase.zookeeper.quorum", this.zookeeperQuorum
            setInt "hbase.zookeeper.property.clientPort", this.zookeeperPort
        }

        connection = ConnectionFactory.createConnection(configuration)
    }

    def getTable(byte[] tableName) {
        connection.getTable TableName.valueOf(namespace, tableName)
    }

    def putCell(byte[] tableName, byte[] key, long timestamp, byte[] value) {
        def table = getTable tableName

        def request = new Put(key, timestamp)
        request.addColumn(family, qualifier, value)

        table.put request
    }

    def getCellAfterTimestamp(byte[] tableName, byte[] key, long timestamp) {
        def table = getTable tableName

        def request = new Get(key)
        request.setTimeRange timestamp, TimeRange.INITIAL_MAX_TIMESTAMP

        def result = table.get request
        result.getValue family, qualifier
    }

    def getCellBeforeTimestamp(byte[] tableName, byte[] key, long timestamp) {
        def table = getTable tableName

        def request = new Get(key)
        request.setTimeRange 0, timestamp

        def result = table.get request
        result.getValue family, qualifier
    }
}
