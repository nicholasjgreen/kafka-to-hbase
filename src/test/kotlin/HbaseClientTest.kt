import com.nhaarman.mockitokotlin2.*
import io.kotlintest.shouldBe
import io.kotlintest.shouldThrow
import io.kotlintest.specs.StringSpec
import org.apache.hadoop.hbase.NamespaceDescriptor
import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.client.Admin
import org.apache.hadoop.hbase.client.Connection
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.client.Table
import java.io.IOException
import java.nio.ByteBuffer
import org.apache.hadoop.hbase.HColumnDescriptor
import org.apache.hadoop.hbase.HTableDescriptor
import org.apache.hadoop.hbase.io.compress.Compression.Algorithm

class HbaseClientTest : StringSpec({
    val columnFamily = "cf".toByteArray()
    val columnQualifier = "record".toByteArray()
    val tableQualifier = "table"
    val regionReplication = 3
    val namespace = "ns"
    val key = "key".toByteArray()
    val body = "body".toByteArray()
    val version = 1L
    val qualifiedTableName = "$namespace:$tableQualifier"
    val tableName = TableName.valueOf(ByteBuffer.wrap(namespace.toByteArray()), ByteBuffer.wrap(tableQualifier.toByteArray()))
    val errorMessage = "ERROR"
    val namespaceDescriptor = mock<NamespaceDescriptor> {
        on { name } doReturn namespace
    }

    val adm = mock<Admin> {
        on { listNamespaceDescriptors() } doReturn arrayOf(namespaceDescriptor)
        on { listTableNames() } doReturn arrayOf(tableName)
    }

    "Does not retry on success" {

        val table = mock<Table> {
            on { exists(any())} doReturn true
        }

        val connection = mock<Connection> {
            on { admin } doReturn adm
            on { getTable(TableName.valueOf(qualifiedTableName)) } doReturn table
        }

        with (HbaseClient(connection, columnFamily, columnQualifier, regionReplication)) {
            put(qualifiedTableName, key, body, version)
        }

        verify(table, times(1)).put(any<Put>())
        verify(table, times(1)).close()
    }

    "Retries until successful put" {
        val table = mock<Table> {
            on { put(any<Put>()) } doThrow IOException(errorMessage) doAnswer {
                println("PUT SUCCEEDED")
            }

            on { exists(any()) } doReturn true
        }

        val connection = mock<Connection> {
            on { admin } doReturn adm
            on { getTable(TableName.valueOf(qualifiedTableName)) } doReturn table
        }

        with (HbaseClient(connection, columnFamily, columnQualifier, regionReplication)) {
            put(qualifiedTableName, key, body, version)
        }

        verify(table, times(2)).put(any<Put>())
        verify(table, times(2)).close()
    }

    "Retries until exists" {
        val table = mock<Table> {
            on { put(any<Put>()) } doAnswer {
                println("PUT APPARENTLY SUCCEEDED")
            }

            on { exists(any()) } doReturnConsecutively listOf(false, true)
        }

        val connection = mock<Connection> {
            on { admin } doReturn adm
            on { getTable(TableName.valueOf(qualifiedTableName)) } doReturn table
        }

        with (HbaseClient(connection, columnFamily, columnQualifier, regionReplication)) {
            put(qualifiedTableName, key, body, version)
        }

        verify(table, times(2)).put(any<Put>())
        verify(table, times(1)).close()
    }

    "Fails after max existence checks exceeded" {
        val table = mock<Table> {
            on { put(any<Put>()) } doAnswer {
                println("PUT SEEMINGLY SUCCEEDED")
            }

            on { exists(any()) } doReturn false
        }

        val connection = mock<Connection> {
            on { admin } doReturn adm
            on { getTable(TableName.valueOf(qualifiedTableName)) } doReturn table
        }

        val exception = shouldThrow<Exception> {
           with(HbaseClient(connection, columnFamily, columnQualifier, regionReplication)) {
                put(qualifiedTableName, key, body, version)
            }
        }

        verify(table, times( 3 * 3)).put(any<Put>())
        verify(table, times(3)).close()
    }

    "Fails after max tries" {

        val maxRetries = 3
        val expectedRetryMaxAttempts = Config.Hbase.retryMaxAttempts

        val table = mock<Table> {
            on { put(any<Put>()) } doThrow IOException(errorMessage)
        }

        val connection = mock<Connection> {
            on { admin } doReturn adm
            on { getTable(TableName.valueOf(qualifiedTableName)) } doReturn table
        }

        val exception = shouldThrow<IOException> {
            with (HbaseClient(connection, columnFamily, columnQualifier, regionReplication)) {
                put(qualifiedTableName, key, body, version)
            }
        }

        exception.message shouldBe errorMessage
        verify(table, times(maxRetries)).put(any<Put>())
        verify(table, times(maxRetries)).close()
        verify(table, times(expectedRetryMaxAttempts)).put(any<Put>())
    }

    "Table not created" {
        val connection = mock<Connection> {
            on { admin } doReturn adm
        }

        val hbaseClient = HbaseClient(connection, "cf".toByteArray(), "record".toByteArray(), 2)
        hbaseClient.ensureTable("$namespace:$tableQualifier")
        verify(adm, times(0)).createNamespace(any())
        verify(adm, times(0)).createTable(any())
    }

    "Namespace and table created" {
        val connection = mock<Connection> {
            on { admin } doReturn adm
        }

        val dataFamily = "cf".toByteArray()
        val dataQualifier = "record".toByteArray()
        val hbaseRegionReplication = 3
        val hbaseClient = HbaseClient(connection, dataFamily, dataQualifier, hbaseRegionReplication)
        val newNamespace = "ns2"
        val newTableQualifier = "table2"
        val newQualifiedTableName = "$newNamespace:$newTableQualifier"
        hbaseClient.ensureTable(newQualifiedTableName)

        verify(adm, times(1)).createNamespace(any())

        val tableDescriptor = HTableDescriptor(TableName.valueOf(newQualifiedTableName)).apply {
            addFamily(HColumnDescriptor(dataFamily)
                    .apply {
                        maxVersions = Int.MAX_VALUE
                        minVersions = 1
                        compressionType = Algorithm.GZ
                        compactionCompressionType = Algorithm.GZ
                    })
            setRegionReplication(hbaseRegionReplication)
        }

        verify(adm, times(1)).createTable(tableDescriptor)
    }

    "Namespace not created but table is created" {
        val newNamespaceDescriptor = mock<NamespaceDescriptor> {
            on { name } doReturn namespace
        }

        val newAdm = mock<Admin> {
            on { listNamespaceDescriptors() } doReturn arrayOf(newNamespaceDescriptor)
            on { listTableNames() } doReturn arrayOf(tableName)
        }

        val connection = mock<Connection> {
            on { admin } doReturn newAdm
        }

        val dataFamily = "cf".toByteArray()
        val dataQualifier = "record".toByteArray()
        val hbaseRegionReplication = 3
        val hbaseClient = HbaseClient(connection, dataFamily, dataQualifier, hbaseRegionReplication)
        val newTableQualifier = "table2"
        val newQualifiedTableName = "$namespace:$newTableQualifier"
        hbaseClient.ensureTable(newQualifiedTableName)

        verify(newAdm, times(0)).createNamespace(any())

        val tableDescriptor = HTableDescriptor(TableName.valueOf(newQualifiedTableName)).apply {
            addFamily(HColumnDescriptor(dataFamily)
                    .apply {
                        maxVersions = Int.MAX_VALUE
                        minVersions = 1
                        compressionType = Algorithm.GZ
                        compactionCompressionType = Algorithm.GZ
                    })
            setRegionReplication(hbaseRegionReplication)
        }

        verify(newAdm, times(1)).createTable(tableDescriptor)
    }
})
