import io.kotest.core.spec.style.StringSpec
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertTrue

class ShovelTest : StringSpec() {

    init {

        // todo: new ticket for refactoring to allow mocking of logger
//        "should print logs when valid batchCount" {
//            val slf4jLogger = mock<Logger> {
//                on { isInfoEnabled } doReturn true
//            }
//
//            val logger = mock<JsonLoggerWrapper> {
//                UseConstructor.withArguments("delegateLogger", slf4jLogger)
//            }
//
//            val offsets = mutableMapOf<String, Long>()
//            val userPartitions = mutableMapOf<String, MutableSet<Int>>()
//
//            printLogs(offsets, userPartitions)
//
//            verify(logger, times(1)).info("Total number of topics", "number_of_topics", offsets.size.toString())
//        }

        "batchCount is a multiple of reportFrequency" {
            val batchCount = 100
            val isMultiple = batchCountIsMultipleOfReportFrequency(batchCount)

            assertTrue(isMultiple)
        }

        "batchCount is not a multiple of reportFrequency" {
            val batchCount = 101
            val isMultiple = batchCountIsMultipleOfReportFrequency(batchCount)

            assertFalse(isMultiple)
        }
    }
}
