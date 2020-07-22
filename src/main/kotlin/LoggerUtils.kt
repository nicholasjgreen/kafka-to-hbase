import ch.qos.logback.classic.spi.ILoggingEvent
import ch.qos.logback.classic.spi.ThrowableProxyUtil
import ch.qos.logback.core.CoreConstants
import ch.qos.logback.core.LayoutBase
import org.apache.commons.text.StringEscapeUtils
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import java.net.InetAddress
import java.text.SimpleDateFormat
import java.util.*

private const val UNSET_TEXT = "NOT_SET"
private val defaultFormat = makeUtcDateFormat() // 2001-07-04T12:08:56.235

private var hostname = InetAddress.getLocalHost().hostName

private var environment = System.getenv("K2HB_ENVIRONMENT") ?: UNSET_TEXT
private var app_version = System.getenv("K2HB_APP_VERSION") ?: UNSET_TEXT
private var component = System.getenv("K2HB_JAR_COMPONENT_NAME") ?: "jar_file"
private var application = System.getenv("K2HB_APPLICATION_NAME") ?: "Kafka2Hbase"
private var instanceId = System.getenv("INSTANCE_ID") ?: UNSET_TEXT
private var staticData = makeLoggerStaticDataTuples()

class LogConfiguration {
    companion object {
        var start_time_milliseconds = System.currentTimeMillis()
    }
}

fun makeUtcDateFormat(): SimpleDateFormat {
    // 2001-07-04T12:08:56.235
    val df = SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS")
    df.timeZone = TimeZone.getTimeZone("UTC")
    return df
}

fun makeLoggerStaticDataTuples(): String {
    return "\"hostname\":\"$hostname\", " +
        "\"environment\":\"$environment\", " +
        "\"application\":\"$application\", " +
        "\"app_version\":\"$app_version\", " +
        "\"component\":\"$component\", " +
        "\"instance_id\":\"$instanceId\", " +
        "\"column_family\":\"${Config.Hbase.columnFamily}\", " +
        "\"column_qualifier\":\"${Config.Hbase.columnQualifier}\", " +
        "\"region_replication\":\"${Config.Hbase.regionReplication}\""
}

fun resetLoggerStaticFieldsForTests() {
    hostname = InetAddress.getLocalHost().hostName
    environment = System.getProperty("environment", UNSET_TEXT)
    application = System.getProperty("application", UNSET_TEXT)
    app_version = System.getProperty("app_version", UNSET_TEXT)
    component = System.getProperty("component", UNSET_TEXT)
    staticData = makeLoggerStaticDataTuples()
}

fun overrideLoggerStaticFieldsForTests(host: String, env: String, app: String, version: String, comp: String, start_milliseconds: String) {
    hostname = host
    environment = env
    application = app
    app_version = version
    component = comp
    LogConfiguration.start_time_milliseconds = start_milliseconds.toLong()
    staticData = makeLoggerStaticDataTuples()
}

fun semiFormattedTuples(message: String, vararg tuples: String): String {
    val semiFormatted = StringBuilder(StringEscapeUtils.escapeJson(message))
    if (tuples.isEmpty()) {
        return semiFormatted.toString()
    }
    if (tuples.size % 2 != 0) {
        throw IllegalArgumentException("Must have matched key-value pairs but had ${tuples.size} argument(s)")
    }
    for (i in tuples.indices step 2) {
        val key = tuples[i]
        val value = tuples[i + 1]
        val escapedValue = StringEscapeUtils.escapeJson(value)
        semiFormatted.append("\", \"")
        semiFormatted.append(key)
        semiFormatted.append("\":\"")
        semiFormatted.append(escapedValue)
    }
    return semiFormatted.toString()
}

fun formattedTimestamp(epochTime: Long): String {
    synchronized(defaultFormat) {
        val netDate = Date(epochTime)
        return defaultFormat.format(netDate)
    }
}

fun flattenMultipleLines(text: String?): String {
    if (text == null) {
        return "null"
    }
    return try {
        text.replace("\n", " | ").replace("\t", " ")
    }
    catch (ex: java.lang.Exception) {
        text
    }
}

fun inlineStackTrace(text: String): String {
    return try {
        StringEscapeUtils.escapeJson(flattenMultipleLines(text))
    }
    catch (ex: java.lang.Exception) {
        text
    }
}

fun throwableProxyEventToString(event: ILoggingEvent): String {
    val throwableProxy = event.throwableProxy
    return if (throwableProxy != null) {
        val stackTrace = ThrowableProxyUtil.asString(throwableProxy)
        val oneLineTrace = inlineStackTrace(stackTrace)
        "\"exception\":\"$oneLineTrace\", "
    }
    else {
        ""
    }
}

open class JsonLoggerWrapper(private val delegateLogger: Logger) {

    companion object {
        fun getLogger(forClassName: String): JsonLoggerWrapper {
            val slf4jLogger: Logger = LoggerFactory.getLogger(forClassName)
            return JsonLoggerWrapper(slf4jLogger)
        }
    }

    fun debug(message: String, vararg tuples: String) {
        if (delegateLogger.isDebugEnabled) {
            val semiFormatted = semiFormattedTuples(message, *tuples)
            delegateLogger.debug(semiFormatted)
        }
    }

    fun info(message: String, vararg tuples: String) {
        if (delegateLogger.isInfoEnabled) {
            val semiFormatted = semiFormattedTuples(message, *tuples)
            delegateLogger.info(semiFormatted)
        }
    }

    fun warn(message: String, vararg tuples: String) {
        if (delegateLogger.isWarnEnabled) {
            val semiFormatted = semiFormattedTuples(message, *tuples)
            delegateLogger.warn(semiFormatted)
        }
    }

    fun error(message: String, vararg tuples: String) {
        if (delegateLogger.isErrorEnabled) {
            val semiFormatted = semiFormattedTuples(message, *tuples)
            delegateLogger.error(semiFormatted)
        }
    }

    fun error(message: String, error: Throwable, vararg tuples: String) {
        if (delegateLogger.isErrorEnabled) {
            val semiFormatted = semiFormattedTuples(message, *tuples)
            delegateLogger.error(semiFormatted, error)
        }
    }
}

class LoggerLayoutAppender : LayoutBase<ILoggingEvent>() {

    override fun doLayout(event: ILoggingEvent?): String {
        if (event == null) {
            return ""
        }
        val dateTime = formattedTimestamp(event.timeStamp)
        val builder = StringBuilder()
        builder.append("{ ")
        builder.append("\"timestamp\":\"")
        builder.append(dateTime)
        builder.append("\", \"log_level\":\"")
        builder.append(event.level)
        builder.append("\", \"message\":\"")
        builder.append(flattenMultipleLines(event.formattedMessage))
        builder.append("\", ")
        builder.append(throwableProxyEventToString(event))
        builder.append("\"thread\":\"")
        builder.append(event.threadName)
        builder.append("\", \"logger\":\"")
        builder.append(event.loggerName)
        builder.append("\", ")
        builder.append(staticData)
        builder.append(" }")
        builder.append(CoreConstants.LINE_SEPARATOR)
        return builder.toString()
    }
}
