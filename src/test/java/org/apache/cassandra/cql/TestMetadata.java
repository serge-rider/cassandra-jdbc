package org.apache.cassandra.cql;

import org.jkiss.jdbc.cassandra.CassandraConnection;
import org.jkiss.jdbc.cassandra.CassandraPreparedStatement;

import java.sql.*;
import java.util.Properties;
import java.util.Random;

/**
 * Test
 */
public class TestMetadata {

    public static final String[] CQL_LINES_START = {
        "CREATE KEYSPACE Test WITH strategy_class = 'SimpleStrategy'\n" +
            "    AND strategy_options:replication_factor = 1\n",
        "USE Test\n",
        "CREATE COLUMNFAMILY Employee (\n" +
            "    id varint PRIMARY KEY,\n" +
            "    fullName varchar,\n" +
            "    age varint,\n" +
            "    biography text,\n" +
            "    photo blob\n" +
            ") WITH comment='Users sample table'\n" +
            "   AND read_repair_chance = 1.0;",
        "CREATE INDEX nameIndex ON Employee (fullName);"
    };

    public static final String[] CQL_LINES_END = {
        "USE Test\n",
        "DROP COLUMNFAMILY Employee;",
        "DROP KEYSPACE Test"
    };

    public static void main(String[] args) throws Exception
    {

        Class.forName("org.jkiss.jdbc.cassandra.CassandraDriver");
        Properties properties = new Properties();
        //properties.setProperty("cqlVersion", "2.0.0");
        properties.setProperty("structResultSet", "true");
        properties.setProperty("user", "qwdf");
        properties.setProperty("password", "qwdf");
        CassandraConnection connection = (CassandraConnection)DriverManager.getConnection(
            "jdbc:jkiss:cassandra://localhost:9160/system", properties);

        dropSchema(connection, true);
        createSchema(connection);

        try {
            System.out.println("Fill with test data");
            createTestData(connection);

            // Do the test
            System.out.println("Explore DB metadata");
            DatabaseMetaData metaData = connection.getMetaData();
            System.out.println("Database: " + metaData.getDatabaseProductName());
            System.out.println("Database version: " + metaData.getDatabaseProductVersion());


            ResultSet schemas = metaData.getSchemas(null, null);
            try {
                while (schemas.next()) {
                    String schema = schemas.getString("TABLE_SCHEM");
                    System.out.println("\tSchema: " + schema);

                    ResultSet tables = metaData.getTables(null, schema, null, null);
                    try {
                        while (tables.next()) {
                            String tableName = tables.getString("TABLE_NAME");
                            System.out.println("\t\tTable: " + tableName);

                            ResultSet columns = metaData.getColumns(null, schema, tableName, null);
                            try {
                                while (columns.next()) {
                                    String column = columns.getString("COLUMN_NAME");
                                    System.out.println("\t\t\tColumn: " + column);
                                }
                            } finally {
                                columns.close();
                            }
                        }
                    } finally {
                        tables.close();
                    }
                }
            } finally {
                schemas.close();
            }

            CassandraPreparedStatement stat = connection.prepareStatement("select count(*) from system.LocationInfo");
            ResultSet rs = stat.executeQuery();
            rs.next();
            System.out.println("COUNT=" + rs.getInt(1));
            rs.close();

            stat = connection.prepareStatement("select * from system.LocationInfo");
            rs = stat.executeQuery();
            while (rs.next()) {
                System.out.println("ROW ===================");
                ResultSetMetaData rsMetaData = rs.getMetaData();
                for (int i = 0; i < rsMetaData.getColumnCount(); i++) {
                    System.out.println(rsMetaData.getColumnName(i + 1) + "=" + rs.getString(i + 1));
                }
            }
            rs.close();

        } finally {
            //dropSchema(connection, false);
        }

        connection.close();
        System.out.println("Done");
    }

    private static void createTestData(CassandraConnection connection) throws SQLException
    {
        Random rnd = new Random();
        connection.setSchema("Test");

/*
        {
            CassandraPreparedStatement stat = connection.prepareStatement("select * from Users");
            ResultSet rs = stat.executeQuery();
            while (rs.next()) {
                System.out.println("ROW ===================");
                ResultSetMetaData rsMetaData = rs.getMetaData();
                for (int i = 0; i < rsMetaData.getColumnCount(); i++) {
                    System.out.println(rsMetaData.getColumnName(i + 1) + "=" + rs.getString(i + 1));
                }
            }
            rs.close();
        }
*/
        long t = System.currentTimeMillis();
        for (int i = 0; i < 1000; i++) {
            boolean hasExtraCol = rnd.nextBoolean();
            boolean hasFewCol = rnd.nextBoolean();
            String sql;
            int extraColNum = 0;
            if (hasExtraCol) {
                extraColNum = rnd.nextInt(20);
                StringBuilder sqlBuf = new StringBuilder("INSERT INTO Employee(id,fullName,age,biography");
                for (int k = 0;k < extraColNum; k++) sqlBuf.append(",Col_" + k);
                sqlBuf.append(") VALUES(?,?,?,?");
                for (int k = 0;k < extraColNum; k++) sqlBuf.append(",?");
                sqlBuf.append(")");
                sql = sqlBuf.toString();
            } else {
                sql = hasFewCol ?
                    "INSERT INTO Employee(id,fullName) VALUES(?,?)" :
                    "INSERT INTO Employee(id,fullName,age,biography) VALUES(?,?,?,?)";
            }
            CassandraPreparedStatement stat = connection.prepareStatement(sql);
            stat.setInt(1, i);
            stat.setString(2, "User" + i);
            if (hasExtraCol || !hasFewCol) {
                stat.setInt(3, 20 + rnd.nextInt(20));
                stat.setString(4, "BIO2");
            }
            if (hasExtraCol) {
                for (int k = 0;k < extraColNum; k++) {
                    stat.setString(5 + k, String.valueOf(rnd.nextInt()));
                }
            }
            stat.execute();
            if (i > 0 && i % 10000 == 0) {
                System.out.println(i + " rows (" + (System.currentTimeMillis() - t) + "ms)");
            }
        }
        System.out.println("Done (" + (System.currentTimeMillis() - t) + "ms)");
    }

    private static void dropSchema(CassandraConnection connection, boolean quit) throws SQLException
    {
        for (String cql : CQL_LINES_END) {
            //System.out.println(cql);
            PreparedStatement stat = connection.prepareStatement(cql);
            try {
                stat.execute();
            } catch (SQLException e) {
                if (!quit) {
                    e.printStackTrace();
                }
            }
        }
    }

    private static void createSchema(CassandraConnection connection) throws SQLException
    {
        for (String cql : CQL_LINES_START) {
            System.out.println(cql);
            PreparedStatement stat = connection.prepareStatement(cql);
            stat.execute();
        }
    }

}
