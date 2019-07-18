package lib

import java.lang.Thread.sleep
import java.time.Duration
import java.time.LocalDateTime
import java.util.concurrent.TimeoutException

fun <T> waitFor(
    interval: Duration = Duration.ofSeconds(1),
    timeout: Duration = Duration.ofSeconds(30),
    predicate: () -> T
): T {
    val start = LocalDateTime.now()
    val end = start + timeout

    while (true) {
        val result = predicate()
        if (result != null || LocalDateTime.now().isAfter(end)) {
            return result
        }
        sleep(interval.toMillis())
    }
}
