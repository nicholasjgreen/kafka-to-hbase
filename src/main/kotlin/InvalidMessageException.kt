class InvalidMessageException : Exception {
    constructor(message: String, cause: Throwable) : super(message, cause)
}