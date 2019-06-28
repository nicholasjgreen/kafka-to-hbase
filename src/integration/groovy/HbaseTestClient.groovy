import org.apache.hadoop.hbase.Cell
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.client.Connection
import org.apache.hadoop.hbase.client.ConnectionFactory
import org.apache.hadoop.hbase.client.Get
import org.apache.hadoop.hbase.io.TimeRange

class HbaseTestClient {
    private def namespace = "k2hb".getBytes()
    private def family = "cf".getBytes()
    private def qualifier = "data".getBytes()

    private def zookeeperQuorum = "zookeeper"
    private def zookeeperPort = 2181

    private Connection connection

    HbaseTestClient() {
        def conf = HBaseConfiguration.create()
        conf.set("hbase.zookeeper.quorum", zookeeperQuorum)
        conf.setInt("hbase.zookeeper.property.clientPort", zookeeperPort)
        connection = ConnectionFactory.createConnection(conf)
    }

    def getCellAfterTimestamp(byte[] tableName, byte[] key, long timestamp) {


        def table = connection.getTable(TableName.valueOf(namespace, tableName))

        def get = new Get(key)
//        get.addColumn(family, qualifier)
        get.setTimeRange(timestamp, TimeRange.INITIAL_MAX_TIMESTAMP)

        def result = table.get(get)
        return result.getValue(family, qualifier)
    }
}
