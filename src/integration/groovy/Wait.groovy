import java.time.Duration
import java.time.LocalDateTime
import java.util.concurrent.TimeoutException

class Wait {

    static def 'for'(
            Duration interval = Duration.ofSeconds(1),
            Duration timeout = Duration.ofSeconds(30),
            Closure predicate) {
        def start = LocalDateTime.now()
        def end = start + timeout

        while (LocalDateTime.now().isBefore(end)) {
            def result = predicate()
            if (result) {
                return result
            }
            sleep(interval.toMillis())
        }

        throw new TimeoutException(String.format("Predicate did not match within %s", timeout.toString()))
    }
}
