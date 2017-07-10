package org.jkiss.jdbc.cassandra;

/**
 * Error messages
 */
class ErrorMessages {

    protected static final String WAS_CLOSED_CON = "method was called on a closed Connection";
    protected static final String WAS_CLOSED_STMT = "method was called on a closed Statement";
    protected static final String WAS_CLOSED_RSLT = "method was called on a closed ResultSet";
    protected static final String NO_TRANSACTIONS = "the Cassandra implementation does not support transactions";
    protected static final String NO_SERVER = "no Cassandra server is available";
    protected static final String SCHEMA_MISMATCH = "schema does not match across nodes, (try again later)";
    protected static final String NO_GEN_KEYS = "the Cassandra implementation does not currently support returning generated  keys";
    protected static final String NO_BATCH = "the Cassandra implementation does not currently support this batch in Statement";
    protected static final String NO_MULTIPLE = "the Cassandra implementation does not currently support multiple open Result Sets";
    protected static final String NO_RESULTSET = "No ResultSet returned from the CQL statement passed in an 'executeQuery()' method";
    protected static final String NO_UPDATE_COUNT = "No Update Count was returned from the CQL statement passed in an 'executeUpdate()' method";
    protected static final String BAD_KEEP_RSET = "the argument for keeping the current result set : %s is not a valid value";
    protected static final String BAD_TYPE_RSET = "the argument for result set type : %s is not a valid value";
    protected static final String BAD_HOLD_RSET = "the argument for result set holdability : %s is not a valid value";
    protected static final String BAD_FETCH_DIR = "fetch direction value of : %s is illegal";
    protected static final String BAD_AUTO_GEN = "auto key generation value of : %s is illegal";
    protected static final String BAD_FETCH_SIZE = "fetch size of : %s rows may not be negative";
    protected static final String NOT_TRANSLATABLE = "column was stored in %s format which is not translatable to %s";
    protected static final String NOT_BOOLEAN = "string value was neither 'true' nor 'false' :  %s";
    protected static final String HOST_IN_URL = "Connection url must specify a host, e.g., jdbc:jkiss:cassandra://localhost:9170/Keyspace1";
    protected static final String HOST_REQUIRED = "a 'host' name is required to build a Connection";
    protected static final String BAD_KEYSPACE = "Keyspace names must be composed of alphanumerics and underscores (parsed: '%s')";
    protected static final String URI_IS_SIMPLE = "Connection url may only include host, port, and keyspace and version option, e.g., jdbc:jkiss:cassandra://localhost:9170/Keyspace1?version=2.0.0";
    protected static final String NOT_OPTION = "Connection url only support the 'version' option";
    protected static final String VALID_LABELS = "name provided was not in the list of valid column labels: %s";
}
