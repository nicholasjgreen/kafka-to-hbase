import org.apache.log4j.ConsoleAppender
import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.log4j.PatternLayout
import spock.lang.Specification

class TestKafka2Hbase extends Specification {
    def topic = "test-topic".getBytes()

    KafkaTestProducer producer
    HbaseTestClient hbase
    Logger log

    def setup() {
        def consoleAppender = new ConsoleAppender()
        consoleAppender.layout = new PatternLayout("%d [%p|%c|%C{1}] %m%n")
        consoleAppender.threshold = Level.INFO
        consoleAppender.activateOptions()
        Logger.getRootLogger().addAppender(consoleAppender)

        log = Logger.getLogger(TestKafka2Hbase)

        producer = new KafkaTestProducer()
        hbase = new HbaseTestClient()
    }

    def "writes a new record when none has been written before"() {
        given: "a new message identifier by a guid"
        def key = RecordData.uniqueBytes()
        def body = RecordData.uniqueBytes()
        def timestamp = RecordData.timestamp()

        when: "the message is pushed to kafka"
        producer.sendRecord(topic, key, body, timestamp)

        then: "the record is written to hbase"
        def value = Wait.forPredicate() {
            hbase.getCellAfterTimestamp(topic, key, timestamp)
        }

        value == body
    }
}