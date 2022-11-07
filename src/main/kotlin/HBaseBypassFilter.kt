open class HBaseBypassFilter (private val bypassTopics: String?) {

    private val bypassTopicRegex: Regex? = if (bypassTopics != null) Regex(bypassTopics) else null

    open fun tableShouldWriteToHBase(table: String): Boolean {
        return !(bypassTopicRegex?.containsMatchIn(table) ?: false)
    }
}
