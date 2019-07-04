import org.apache.log4j.ConsoleAppender
import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.log4j.PatternLayout
import spock.lang.Specification

class TestKafka2Hbase extends Specification {
    byte[] topic = "test-topic".getBytes()

    KafkaTestProducer producer
    HbaseTestClient hbase
    Logger log

    def setup() {
        def appender = new ConsoleAppender()
        appender.with {
            layout = new PatternLayout("%d [%p|%c|%C{1}] %m%n")
            threshold = Level.INFO
            activateOptions()
        }

        Logger.getRootLogger().addAppender appender

        producer = new KafkaTestProducer()
        hbase = new HbaseTestClient()
    }

    def "writes a new record when none has been written before"() {
        given: "a new message identifier by a guid"
        def key = RecordData.uniqueBytes()
        def body = RecordData.uniqueBytes()
        def timestamp = RecordData.timestamp()

        when: "the message is pushed to kafka"
        producer.sendRecord topic, key, body, timestamp

        then: "the record is written to hbase"
        body == Wait.for() { hbase.getCellAfterTimestamp topic, key, timestamp }
    }

    def "writes new version of an existing record"() {
        given: "an existing message has been received"
        def key = RecordData.uniqueBytes()
        def firstBody = RecordData.uniqueBytes()
        def firstTimestamp = RecordData.timestamp()
        hbase.putCell topic, key, firstTimestamp, firstBody

        when: "a new version of the same record is pushed to kafka"
        def secondBody = RecordData.uniqueBytes()
        def secondTimestamp = firstTimestamp + 1
        producer.sendRecord topic, key, secondBody, secondTimestamp

        then: "the record is written to hbase as an additional version"
        secondBody == Wait.for() { hbase.getCellAfterTimestamp topic, key, secondTimestamp }

        and: "the original version is still available"
        firstBody == Wait.for() { hbase.getCellBeforeTimestamp topic, key, secondTimestamp }
    }
}