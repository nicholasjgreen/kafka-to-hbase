import org.apache.log4j.ConsoleAppender
import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.log4j.PatternLayout

fun configureLogging() {
    val consoleAppender = ConsoleAppender()
    consoleAppender.layout = PatternLayout("%d [%p|%c|%C{1}] %m%n")
    consoleAppender.threshold = Level.INFO
    consoleAppender.activateOptions()
    Logger.getRootLogger().addAppender(consoleAppender)
}
