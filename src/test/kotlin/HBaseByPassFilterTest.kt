import io.kotest.core.spec.style.StringSpec

class HBaseByPassFilterTest: StringSpec() {

    val GOOD_REGEX = "^(database[.]{1}[-\\w]+[.]{1}[-\\w]+)$"
    val ALL_REGEX = ".*"

    init {
        "Does not bypass any tables when regex not set" {
            val filter = HBaseBypassFilter(null);

            // Everything should be written to HBase
            assert(filter.tableShouldWriteToHBase("database.collection1"))
            assert(filter.tableShouldWriteToHBase("database.collection2"))
            assert(filter.tableShouldWriteToHBase("database.collection3.part1"))
            assert(filter.tableShouldWriteToHBase("something"))
            assert(filter.tableShouldWriteToHBase(""))
            assert(filter.tableShouldWriteToHBase("324234324"))
            assert(filter.tableShouldWriteToHBase("12/12/12"))
        }

        "Does not bypass any tables when regex not valid" {
            val filter = HBaseBypassFilter("not a regular expression");

            // Everything should be written to hbase
            assert(filter.tableShouldWriteToHBase("database.collection1"))
            assert(filter.tableShouldWriteToHBase("database.collection2"))
            assert(filter.tableShouldWriteToHBase("database.collection3.part1"))
            assert(filter.tableShouldWriteToHBase("something"))
            assert(filter.tableShouldWriteToHBase(""))
            assert(filter.tableShouldWriteToHBase("324234324"))
            assert(filter.tableShouldWriteToHBase("12/12/12"))
        }

        "Does not bypass any tables when regex does not match table name" {
            // A good expression that matches real collections for "database"
            val filter = HBaseBypassFilter(GOOD_REGEX);

            // not part of "database" and should be written to hbase
            assert(filter.tableShouldWriteToHBase("stream.main.collection"))
            assert(filter.tableShouldWriteToHBase("stream.extra.part1.section"))

            // part of "database", but does not match pattern and should be written to hbase
            assert(filter.tableShouldWriteToHBase("database.something"))
            assert(filter.tableShouldWriteToHBase("database.collection3.part1.section"))

            // Invalid table names, should be written to hbase
            assert(filter.tableShouldWriteToHBase("something"))
            assert(filter.tableShouldWriteToHBase(""))
            assert(filter.tableShouldWriteToHBase("324234324"))
            assert(filter.tableShouldWriteToHBase("12/12/12"))
        }

        "Does bypass tables when regex matches" {
            // A good expression that matches real collections
            val filter = HBaseBypassFilter(GOOD_REGEX);

            // Real collections that match, should NOT be written to hbase
            assert(!filter.tableShouldWriteToHBase("database.collection1.part0"))
            assert(!filter.tableShouldWriteToHBase("database.collection2.part0"))
            assert(!filter.tableShouldWriteToHBase("database.collection3.part1"))
        }

        "Does bypass tables when regex is wildcard" {
            // An expression that matches everything
            val filter = HBaseBypassFilter(ALL_REGEX);

            // Real collections that match, should NOT be written to hbase
            assert(!filter.tableShouldWriteToHBase("database.collection1.part0"))
            assert(!filter.tableShouldWriteToHBase("database.collection2.part0"))
            assert(!filter.tableShouldWriteToHBase("database.collection3.part1"))

            // not part of "database" and should NOT be written to hbase
            assert(!filter.tableShouldWriteToHBase("stream.main.collection"))
            assert(!filter.tableShouldWriteToHBase("stream.extra.part1.section"))

            // Invalid table names, should match anyway and NOT be written to hbase
            assert(!filter.tableShouldWriteToHBase("database.something"))
            assert(!filter.tableShouldWriteToHBase("database.collection3.part1.section"))
            assert(!filter.tableShouldWriteToHBase("something"))
            assert(!filter.tableShouldWriteToHBase(""))
            assert(!filter.tableShouldWriteToHBase("324234324"))
            assert(!filter.tableShouldWriteToHBase("12/12/12"))
        }
    }
}
