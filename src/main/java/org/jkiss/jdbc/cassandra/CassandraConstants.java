package org.jkiss.jdbc.cassandra;

/**
 * Constants
 */
public class CassandraConstants {
    public static final String PROTOCOL = "jdbc:jkiss:cassandra:";
    public static final int DEFAULT_PORT = 9160;

    public static final String PROP_USER = "user";
    public static final String PROP_PASSWORD = "password";
    public static final String PROP_DATABASE_NAME = "databaseName";
    public static final String PROP_SERVER_NAME = "serverName";
    public static final String PROP_PORT_NUMBER = "portNumber";
    public static final String PROP_ACTIVE_CQL_VERSION = "activeCqlVersion";
    public static final String PROP_CQL_VERSION = "cqlVersion";
    public static final String PROP_STRUCT_RESULT_SET = "structResultSet";

    public static final String DB_PRODUCT_NAME = "Cassandra";

    public static final int DRIVER_MAJOR_VERSION = 1;
    public static final int DRIVER_MINOR_VERSION = 0;
    public static final int DRIVER_PATCH_VERSION = 0;
    public static final String DRIVER_NAME = "Cassandra JKISS Driver";

    public static final Object CF_TABLE_TYPE = "COLUMN FAMILY";
    public static final String DEFAULT_KEY_ALIAS = "KEY";
    public static final String DEFAULT_KEYSPACE = "system";
    public static final String ROW_COLUMN_NAME = "ROW";
    public static final String ROW_TYPE_NAME = "ROW";

}
