import com.nhaarman.mockitokotlin2.*
import io.kotlintest.should
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


class HbaseClientTest : StringSpec({
    val columnFamily = "cf".toByteArray()
    val columnQualifier = "record".toByteArray()
    val tableQualifier = "table"
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
        val table = mock<Table>()
        val connection = mock<Connection> {
            on { admin } doReturn adm
            on { getTable(TableName.valueOf(qualifiedTableName)) } doReturn table
        }

        with (HbaseClient(connection, columnFamily, columnQualifier)) {
            put(qualifiedTableName, key, body, version)
        }

        verify(table, times(1)).put(any<Put>())
    }

    "Retries until successful put" {
        val table = mock<Table> {
            on { put(any<Put>()) } doThrow IOException(errorMessage) doAnswer {
                println("PUT SUCCEEDED")
            }
        }

        val connection = mock<Connection> {
            on { admin } doReturn adm
            on { getTable(TableName.valueOf(qualifiedTableName)) } doReturn table
        }

        with (HbaseClient(connection, columnFamily, columnQualifier)) {
            put(qualifiedTableName, key, body, version)
        }

        verify(table, times(2)).put(any<Put>())
    }

    "Fails after max tries" {
        val table = mock<Table> {
            on { put(any<Put>()) } doThrow IOException(errorMessage)
        }

        val connection = mock<Connection> {
            on { admin } doReturn adm
            on { getTable(TableName.valueOf(qualifiedTableName)) } doReturn table
        }

        val exception = shouldThrow<IOException> {
            with (HbaseClient(connection, columnFamily, columnQualifier)) {
                put(qualifiedTableName, key, body, version)
            }
        }

        exception.message shouldBe errorMessage
        verify(table, times(System.getenv("K2HB_RETRY_MAX_ATTEMPTS").toInt())).put(any<Put>())
    }
})
