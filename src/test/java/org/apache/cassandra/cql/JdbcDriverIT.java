package org.apache.cassandra.cql;
/*
 * 
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 * 
 */

import java.io.File;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.sql.*;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import org.jkiss.jdbc.cassandra.CassandraUtils;
import org.jkiss.jdbc.cassandra.types.*;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import static junit.framework.Assert.assertEquals;

/**
 * Test case for unit test of various methods of JDBC implementation.
 */
public class JdbcDriverIT
{
    private static final String HOST = System.getProperty("host", ConnectionDetails.getHost());
    private static final int PORT = Integer.parseInt(System.getProperty("port", ConnectionDetails.getPort()+""));
    
    private static java.sql.Connection con = null;
    private static final String first = CassandraUtils.bytesToHex("first".getBytes());
    private static final String firstrec = CassandraUtils.bytesToHex("firstrec".getBytes());
    private static final String last = CassandraUtils.bytesToHex("last".getBytes());
    private static final String lastrec = CassandraUtils.bytesToHex("lastrec".getBytes());
    private static final String jsmith = CassandraUtils.bytesToHex("jsmith".getBytes());
    private static final Schema schema = new Schema(HOST, PORT);

    /** SetUp */
    @BeforeClass
    public static void startServer() throws Exception
    {
        schema.createSchema();
        Class.forName("org.apache.cassandra.cql.jdbc.CassandraDriver");
        con = DriverManager.getConnection(String.format("jdbc:jkiss:cassandra://%s:%d/%s",HOST, PORT, Schema.KEYSPACE_NAME));
        String[] inserts = 
        {
            String.format("UPDATE Standard1 SET '%s' = '%s', '%s' = '%s' WHERE KEY = '%s'", first, firstrec, last, lastrec, jsmith),    
            "UPDATE JdbcInteger0 SET 1 = 11, 2 = 22, 42='fortytwo' WHERE KEY = '" + jsmith + "'",
            "UPDATE JdbcInteger0 SET 3 = 33, 4 = 44 WHERE KEY = '" + jsmith + "'",
            "UPDATE JdbcInteger1 SET 1 = 'One', 2 = 'Two', 3 = 'Three' WHERE id = rowOne",
            "UPDATE JdbcLong SET 1 = 11, 2 = 22 WHERE KEY = '" + jsmith + "'",
            "UPDATE JdbcAscii SET 'first' = 'firstrec', last = 'lastrec' WHERE key = '" + jsmith + "'",
            String.format("UPDATE JdbcBytes SET '%s' = '%s', '%s' = '%s' WHERE key = '%s'", first, firstrec, last, lastrec, jsmith),
            "UPDATE JdbcUtf8 SET 'first' = 'firstrec', fortytwo = '42', last = 'lastrec' WHERE key = '" + jsmith + "'",
        };
        for (String q : inserts)
        {
            try 
            {
                executeNoResults(con, q);
            }
            catch (SQLException ex)
            {
                throw new AssertionError(ex.getMessage() + ", query:" + q);
            }
        }
    }

    private static void expectedMetaData(ResultSetMetaData md, int col, String colClass, String table, String schema,
                                  String label, int type, String typeName, boolean signed, boolean caseSensitive) throws SQLException
    {
        assertEquals(colClass, md.getColumnClassName(col)); // full class name of type<T>
        assertEquals(schema, md.getSchemaName(col));
        assertEquals(label, md.getColumnLabel(col));
        assertEquals(label, md.getColumnName(col));
        assertEquals(type, md.getColumnType(col));
        assertEquals(typeName, md.getColumnTypeName(col));
        assertEquals(signed, md.isSigned(col));
        assertEquals(caseSensitive, md.isCaseSensitive(col));
    }
    
    private static void expectedMetaData(ResultSetMetaData md, int col,
                                         String valuClass, int valuType, String valuTypeName, boolean valuSigned, boolean valuCaseSense) throws SQLException
    {
        assertEquals(valuClass, md.getColumnClassName(col));
        assertEquals(valuType, md.getColumnType(col));
        assertEquals(valuTypeName, md.getColumnTypeName(col));
        assertEquals(valuSigned, md.isSigned(col));
        assertEquals(valuCaseSense, md.isCaseSensitive(col));
    }

    @Test
    public void testNoHost() throws SQLException
    {
        try
        {
            DriverManager.getConnection("jdbc:jkiss:cassandra:localhost");
        }
        catch (Exception e)
        {
            assertEquals(SQLNonTransientConnectionException.class, e.getClass());
        }
    }

    @Test
    public void testBadKeyspace() throws SQLException
    {
        try
        {
            DriverManager.getConnection("jdbc:jkiss:cassandra://localhost/Keysp@ce");
        }
        catch (Exception e)
        {
            assertEquals(SQLNonTransientConnectionException.class, e.getClass());
        }
    }

    @Test
    public void testBadUserinfo() throws SQLException
    {
        try
        {
            DriverManager.getConnection("jdbc:jkiss:cassandra://root;root@localhost");
        }
        catch (Exception e)
        {
            assertEquals(SQLNonTransientConnectionException.class, e.getClass());
        }
    }

    @Test
    public void testNonDefaultColumnValidators() throws SQLException
    {
        String key = CassandraUtils.bytesToHex("Integer".getBytes());
        Statement stmt = con.createStatement();
        stmt.executeUpdate("update JdbcInteger0 set 1=36893488147419103232, 42='fortytwofortytwo' where key='" + key + "'");
        ResultSet rs = stmt.executeQuery("select 1, 2, 42 from JdbcInteger0 where key='" + key + "'");
        assert rs.next();
        assert rs.getObject("1").equals(new BigInteger("36893488147419103232"));
        assert rs.getString("42").equals("fortytwofortytwo") : rs.getString("42");
        
        ResultSetMetaData md = rs.getMetaData();
        assert md.getColumnCount() == 3;
        expectedMetaData(md, 1, BigInteger.class.getName(), "JdbcInteger", Schema.KEYSPACE_NAME, "1", Types.BIGINT, JdbcInteger.class.getSimpleName(), true, false);
        expectedMetaData(md, 2, BigInteger.class.getName(), "JdbcInteger", Schema.KEYSPACE_NAME, "2", Types.BIGINT, JdbcInteger.class.getSimpleName(), true, false);
        expectedMetaData(md, 3, String.class.getName(), "JdbcInteger", Schema.KEYSPACE_NAME, "42", Types.VARCHAR, JdbcUTF8.class.getSimpleName(), false, true);
        
        rs = stmt.executeQuery("select key, 1, 2, 42 from JdbcInteger0 where key='" + key + "'");
        assert rs.next();
        assert rs.getObject("1").equals(new BigInteger("36893488147419103232"));
        assert rs.getString("42").equals("fortytwofortytwo") : rs.getString("42");

        stmt.executeUpdate("update JdbcUtf8 set a='aa', b='bb', fortytwo='4242' where key='" + key + "'");
        rs = stmt.executeQuery("select a, b, fortytwo from JdbcUtf8 where key='" + key + "'");
        assert rs.next();
        assert rs.getString("a").equals("aa");
        assert rs.getString("b").equals("bb");
        assert rs.getInt("fortytwo") == 4242L;
        
        md = rs.getMetaData();
        expectedMetaData(md, 1, String.class.getName(), "JdbcUtf8", Schema.KEYSPACE_NAME, "a", Types.VARCHAR, JdbcUTF8.class.getSimpleName(), false, true);
        expectedMetaData(md, 2, String.class.getName(), "JdbcUtf8", Schema.KEYSPACE_NAME, "b", Types.VARCHAR, JdbcUTF8.class.getSimpleName(), false, true);
        expectedMetaData(md, 3, BigInteger.class.getName(), "JdbcUtf8", Schema.KEYSPACE_NAME, "fortytwo", Types.BIGINT, JdbcInteger.class.getSimpleName(), true, false);
    }
        
    @Test
    public void testLongMetadata() throws SQLException
    {
        String key = CassandraUtils.bytesToHex("Long".getBytes());
        Statement stmt = con.createStatement();
        stmt.executeUpdate("UPDATE JdbcLong SET 1=111, 2=222 WHERE KEY = '" + key + "'");
        ResultSet rs = stmt.executeQuery("SELECT 1, 2 from JdbcLong WHERE KEY = '" + key + "'");
        assert rs.next();
        assert rs.getLong("1") == 111;
        assert rs.getLong("2") == 222;
        
        ResultSetMetaData md = rs.getMetaData();
        assert md.getColumnCount() == 2;
        expectedMetaData(md, 1, Long.class.getName(), "JdbcLong", Schema.KEYSPACE_NAME, "1", Types.BIGINT, JdbcLong.class.getSimpleName(), true, false);
        expectedMetaData(md, 2, Long.class.getName(), "JdbcLong", Schema.KEYSPACE_NAME, "2", Types.BIGINT, JdbcLong.class.getSimpleName(), true, false);
        
        for (int i = 0; i < md.getColumnCount(); i++)
            expectedMetaData(md, i + 1, Long.class.getName(), Types.BIGINT, JdbcLong.class.getSimpleName(), true, false);
    }

    @Test
    public void testStringMetadata() throws SQLException
    {
        String aKey = CassandraUtils.bytesToHex("ascii".getBytes());
        String uKey = CassandraUtils.bytesToHex("utf8".getBytes());
        Statement stmt = con.createStatement();
        stmt.executeUpdate("UPDATE JdbcAscii SET a='aa', b='bb' WHERE KEY = '" + aKey + "'");
        stmt.executeUpdate("UPDATE JdbcUtf8 SET a='aa', b='bb' WHERE KEY = '" + uKey + "'");
        ResultSet rs0 = stmt.executeQuery("SELECT a, b FROM JdbcAscii WHERE KEY = '" + aKey + "'");
        ResultSet rs1 = stmt.executeQuery("SELECT a, b FROM JdbcUtf8 WHERE KEY = '" + uKey + "'");
        for (ResultSet rs : new ResultSet[] { rs0, rs1 }) 
        {
            assert rs.next();
            assert rs.getString("a").equals("aa");
            assert rs.getString("b").equals("bb");
        }
        
        ResultSetMetaData md = rs0.getMetaData();
        assert md.getColumnCount() == 2;
        expectedMetaData(md, 1, String.class.getName(), "JdbcAscii", Schema.KEYSPACE_NAME, "a", Types.VARCHAR, JdbcAscii.class.getSimpleName(), false, true);
        expectedMetaData(md, 2, String.class.getName(), "JdbcAscii", Schema.KEYSPACE_NAME, "b", Types.VARCHAR, JdbcAscii.class.getSimpleName(), false, true);
        md = rs1.getMetaData();
        assert md.getColumnCount() == 2;
        expectedMetaData(md, 1, String.class.getName(), "JdbcUtf8", Schema.KEYSPACE_NAME, "a", Types.VARCHAR, JdbcUTF8.class.getSimpleName(), false, true);
        expectedMetaData(md, 2, String.class.getName(), "JdbcUtf8", Schema.KEYSPACE_NAME, "b", Types.VARCHAR, JdbcUTF8.class.getSimpleName(), false, true);

        for (int i = 0; i < 2; i++)
        {
            expectedMetaData(rs0.getMetaData(),
                             i + 1,
                             String.class.getName(),
                             Types.VARCHAR,
                             JdbcAscii.class.getSimpleName(),
                             false,
                             true);
            expectedMetaData(rs1.getMetaData(),
                             i + 1,
                             String.class.getName(),
                             Types.VARCHAR,
                             JdbcUTF8.class.getSimpleName(),
                             false,
                             true);

        }
    }
    
    @Test
    public void testBytesMetadata() throws SQLException 
    {
        String key = CassandraUtils.bytesToHex("bytes".getBytes());
        Statement stmt = con.createStatement();
        byte[] a = "a_".getBytes();
        byte[] b = "b_".getBytes();
        byte[] aa = "_aa_".getBytes();
        byte[] bb = "_bb_".getBytes();
        stmt.executeUpdate(String.format(
                "UPDATE JdbcBytes set '%s'='%s', '%s'='%s' WHERE KEY = '" + key + "'",
                CassandraUtils.bytesToHex(a),
                CassandraUtils.bytesToHex(aa),
                CassandraUtils.bytesToHex(b),
                CassandraUtils.bytesToHex(bb)));
        ResultSet rs = stmt.executeQuery(String.format(
                "SELECT '%s', '%s' from JdbcBytes WHERE KEY = '" + key + "'",
                CassandraUtils.bytesToHex(a),
                CassandraUtils.bytesToHex(b)));
        assert rs.next();
        assert Arrays.equals(aa, rs.getBytes(1));
        assert Arrays.equals(bb, rs.getBytes(2));
        assert Arrays.equals(aa, rs.getBytes(CassandraUtils.bytesToHex(a)));
        assert Arrays.equals(bb, rs.getBytes(CassandraUtils.bytesToHex(b)));
        ResultSetMetaData md = rs.getMetaData();
        assert md.getColumnCount() == 2;
        expectedMetaData(md, 1, ByteBuffer.class.getName(), "JdbcBytes", Schema.KEYSPACE_NAME, CassandraUtils.bytesToHex(a), Types.BINARY, JdbcBytes.class.getSimpleName(), false, false);
        expectedMetaData(md, 2, ByteBuffer.class.getName(), "JdbcBytes", Schema.KEYSPACE_NAME, CassandraUtils.bytesToHex(b), Types.BINARY, JdbcBytes.class.getSimpleName(), false, false);
        
        for (int i = 0; i < md.getColumnCount(); i++)
            expectedMetaData(md, i + 1, ByteBuffer.class.getName(), Types.BINARY, JdbcBytes.class.getSimpleName(), false, false);
    }

    @Test
    public void testWithStatementBytesType() throws SQLException
    {
        Statement stmt = con.createStatement();
        
        String selectQ = String.format("SELECT '%s', '%s' FROM Standard1 WHERE KEY='%s'", first, last, jsmith);
        checkResultSet(stmt.executeQuery(selectQ), "Bytes", 1, first, last);
        
        selectQ = String.format("SELECT '%s', '%s' FROM JdbcBytes WHERE KEY='%s'", first, last, jsmith);
        checkResultSet(stmt.executeQuery(selectQ), "Bytes", 1, first, last);
    }
    
    /** Method to test statement. */
//    @Test
//    public void testWithStatement() throws SQLException
//    {
//        Statement stmt = con.createStatement();
//        List<String> keys = Arrays.asList(jsmith);
//        String selectQ = "SELECT 1, 2 FROM JdbcInteger0 WHERE KEY='" + jsmith + "'";
//        checkResultSet(stmt.executeQuery(selectQ), "Int", 1, keys, "1", "2");
//        
//        selectQ = "SELECT 3, 4 FROM JdbcInteger0 WHERE KEY='" + jsmith + "'";
//        checkResultSet(stmt.executeQuery(selectQ), "Int", 1, keys, "3", "4");
//        
//        selectQ = "SELECT 1, 2, 3, 4 FROM JdbcInteger0 WHERE KEY='" + jsmith + "'";
//        checkResultSet(stmt.executeQuery(selectQ), "Int", 1, keys, "1", "2", "3", "4");
//        
//        selectQ = "SELECT 1, 2 FROM JdbcLong WHERE KEY='" + jsmith + "'";
//        checkResultSet(stmt.executeQuery(selectQ), "Long", 1, keys, "1", "2");
//        
//        selectQ = "SELECT 'first', last FROM JdbcAscii WHERE KEY='" + jsmith + "'";
//        checkResultSet(stmt.executeQuery(selectQ), "String", 1, keys, "first", "last");
//        
//        selectQ = String.format("SELECT '%s', '%s' FROM JdbcBytes WHERE KEY='%s'", first, last, jsmith);
//        checkResultSet(stmt.executeQuery(selectQ), "Bytes", 1, keys, first, last);
//        
//        selectQ = "SELECT 'first', last FROM JdbcUtf8 WHERE KEY='" + jsmith + "'";
//        checkResultSet(stmt.executeQuery(selectQ), "String", 1, keys, "first", "last");
//
//        selectQ = "SELECT 1, 2 FROM JdbcInteger1 WHERE id IN (rowOne, badRow)";
//        checkResultSet(stmt.executeQuery(selectQ), "String", 1, keys, "1", "2");
//
////        String badKey = CassandraUtils.bytesToHex(String.format("jsmith-%s", System.currentTimeMillis()).getBytes());
////        selectQ = "SELECT 1, 2 FROM JdbcInteger WHERE KEY IN ('" + badKey + "', '" + jsmith + "')";
////        checkResultSet(stmt.executeQuery(selectQ), "Int", 0, keys, "1", "2");
//    }
    
    @Test
    public void testWithPreparedStatementBytesType() throws SQLException
    {
        String selectQ = String.format("SELECT '%s', '%s' FROM Standard1 WHERE KEY='%s'", first, last, jsmith);
        checkResultSet(executePreparedStatementWithResults(con, selectQ), "Bytes", 1, first, last);
        
        selectQ = String.format("SELECT '%s', '%s' FROM JdbcBytes WHERE KEY='%s'", first, last, jsmith);
        checkResultSet(executePreparedStatementWithResults(con, selectQ), "Bytes", 1, first, last);
    }

   /** Method to test with prepared statement.*/
//   @Test
//    public void testWithPreparedStatement() throws SQLException
//    {
//        List<String> keys = Arrays.asList(jsmith);
//
//        String selectQ = String.format("SELECT '%s', '%s' FROM Standard1 WHERE KEY='%s'", first, last, jsmith);
//        checkResultSet(executePreparedStatementWithResults(con, selectQ), "Bytes", 1, keys, first, last);
//        
//        selectQ = "SELECT 1, 2 FROM JdbcInteger0 WHERE KEY='" + jsmith + "'";
//        checkResultSet(executePreparedStatementWithResults(con, selectQ), "Int", 1, keys, "1", "2");
//        
//        selectQ = "SELECT 3, 4 FROM JdbcInteger0 WHERE KEY='" + jsmith + "'";
//        checkResultSet(executePreparedStatementWithResults(con, selectQ), "Int", 1, keys, "3", "4");
//        
//        selectQ = "SELECT 1, 2, 3, 4 FROM JdbcInteger0 WHERE KEY='" + jsmith + "'";
//        checkResultSet(executePreparedStatementWithResults(con, selectQ), "Int", 1, keys, "1", "2", "3", "4");
//        
//        selectQ = "SELECT 1, 2 FROM JdbcLong WHERE KEY='" + jsmith + "'";
//        checkResultSet(executePreparedStatementWithResults(con, selectQ), "Long", 1, keys, "1", "2");
//        
//        selectQ = "SELECT 'first', last FROM JdbcAscii WHERE KEY='" + jsmith + "'";
//        checkResultSet(executePreparedStatementWithResults(con, selectQ), "String", 1, keys, "first", "last");
//        
//        selectQ = String.format("SELECT '%s', '%s' FROM JdbcBytes WHERE KEY='%s'", first, last, jsmith);
//        checkResultSet(executePreparedStatementWithResults(con, selectQ), "Bytes", 1, keys, first, last);
//        
//        selectQ = "SELECT 'first', last FROM JdbcUtf8 WHERE KEY='" + jsmith + "'";
//        checkResultSet(executePreparedStatementWithResults(con, selectQ), "String", 1, keys, "first", "last");
//
//        selectQ = "SELECT 1, 2 FROM JdbcInteger1 WHERE id IN (rowOne, badRow)";
//        checkResultSet(executePreparedStatementWithResults(con, selectQ), "String", 1, keys, "1", "2");
//
//        String badKey = CassandraUtils.bytesToHex(String.format("jsmith-%s", System.currentTimeMillis()).getBytes());
//        selectQ = "SELECT 1, 2 FROM JdbcInteger WHERE KEY IN ('" + badKey + "', '" + jsmith + "')";
//        checkResultSet(executePreparedStatementWithResults(con, selectQ), "Int", 0, keys, "1", "2");
//    }

    /* Method to test with Delete statement. */
    @Test
    public void testWithDeleteStatement() throws SQLException
    {
        // the pattern: 0) a deltion, 1) ensure deletion 2) ensure deletion wasn't over-eager.
        String[] statements = 
        {
                String.format("DELETE '%s', '%s' FROM Standard1 WHERE KEY='%s'",
                              CassandraUtils.bytesToHex("firstN".getBytes()),
                              CassandraUtils.bytesToHex("lastN".getBytes()),
                              jsmith),
                String.format("SELECT '%s', '%s' FROM Standard1 WHERE KEY='%s'",
                              CassandraUtils.bytesToHex("firstN".getBytes()),
                              CassandraUtils.bytesToHex("lastN".getBytes()),
                              jsmith),
                String.format("SELECT '%s' FROM Standard1 WHERE KEY='%s'",
                              first,
                              jsmith),
                
                "DELETE 1, 3 FROM JdbcInteger0 WHERE KEY='" + jsmith + "'",
                "SELECT 1, 3 FROM JdbcInteger0 WHERE KEY='" + jsmith + "'",
                "SELECT 2, 4 FROM JdbcInteger0 WHERE KEY='" + jsmith + "'",
                
                "DELETE 1 FROM JdbcLong WHERE KEY='" + jsmith + "'",
                "SELECT 1 FROM JdbcLong WHERE KEY='" + jsmith + "'",
                "SELECT 2 FROM JdbcLong WHERE KEY='" + jsmith + "'",
                
                "DELETE 'first' FROM JdbcAscii WHERE KEY='" + jsmith + "'",
                "SELECT 'first' FROM JdbcAscii WHERE KEY='" + jsmith + "'",
                "SELECT last FROM JdbcAscii WHERE KEY='" + jsmith + "'",
                
                String.format("DELETE '%s' FROM JdbcBytes WHERE KEY='%s'", first, jsmith),
                String.format("SELECT '%s' FROM JdbcBytes WHERE KEY='%s'", first, jsmith),
                String.format("SELECT '%s' FROM JdbcBytes WHERE KEY='%s'", last, jsmith),
                
                "DELETE 'first' FROM JdbcUtf8 WHERE KEY='" + jsmith + "'",
                "SELECT 'first' FROM JdbcUtf8 WHERE KEY='" + jsmith + "'",
                "SELECT last FROM JdbcUtf8 WHERE KEY='" + jsmith + "'",
        };
        
        for (int i = 0; i < statements.length/3; i++) 
        {
            executeNoResults(con, statements[3*i]);
            ResultSet rs = executePreparedStatementWithResults(con, statements[3*i+1]);
            rs.next();
            rs.getObject(1);
            assert rs.wasNull();
            rs.close();

            rs = executePreparedStatementWithResults(con, statements[3*i+2]);
            assert rs.next() : statements[3*i+2];
        }
    }

    @AfterClass
    public static void stopServer() throws SQLException
    {
        if (con != null)
        {
            String[] stmts = 
            {
                "TRUNCATE Standard1",
                "TRUNCATE JdbcAscii", // todo: this one is broken for some reason.
                "TRUNCATE JdbcInteger0",
                "TRUNCATE JdbcInteger1",
                "TRUNCATE JdbcLong",
                "TRUNCATE JdbcBytes",
                "TRUNCATE JdbcUtf8",
            };
            for (String stmt : stmts)
            {
                try 
                {
                    executeNoResults(con, stmt);
                }
                catch (SQLException ex)
                {
                    throw new SQLException(stmt, ex);
                }
            }
            schema.dropSchema();
            con.close();
            con = null;
        }
        
        // Cleanup backup links
        File backups = new File("build/test/cassandra/data/Keyspace1/backups");
        if (backups.exists())
            for (String fname : backups.list())
                new File("build/test/cassandra/data/Keyspace1/backups" + File.separator + fname).delete();
    }
    
    // todo: check expected values as well.
    /** iterates over a result set checking columns */
    private static void checkResultSet(ResultSet rs, String accessor, int expectedRows, String... cols) throws SQLException
    {
        checkResultSet(rs, accessor, expectedRows, null, cols);
    }

    private static void checkResultSet(ResultSet rs, String accessor, int expectedRows, List<String> keys,  String... cols) throws SQLException
    {
        int actualRows = 0;
        assert rs != null;
        Iterator<String> keyIter = (keys == null) ? null : keys.iterator();
        ResultSet cassandraRs = (ResultSet)rs;
        while (rs.next())
        {
            actualRows++;

            for (int c = 0; c < cols.length; c++)
            {
                // getObject should always work.
//                assert rs.getObject(cols[c]) != null;
//                assert rs.getObject(c+1) != null;
                
                // now call the accessor.
                try
                {
                    Method byInt = rs.getClass().getDeclaredMethod("get" + accessor, int.class);
                    byInt.setAccessible(true);
                    assert byInt.invoke(rs, c+1) != null;
                    
                    Method byString = rs.getClass().getDeclaredMethod("get" + accessor, String.class);
                    byString.setAccessible(true);
                    assert byString.invoke(rs, cols[c]) != null;
                }
                catch (NoSuchMethodException ex)
                {
                    throw new RuntimeException(ex);
                }
                catch (IllegalAccessException ex)
                {
                    throw new RuntimeException(ex);
                }
                catch (InvocationTargetException ex) 
                {
                    throw new RuntimeException(ex);
                }
            }
        }
        
        assert actualRows == expectedRows : String.format("expected %d rows, got %d", expectedRows, actualRows);
    }
    
    /** executes a prepared statement */
    private static ResultSet executePreparedStatementWithResults(final Connection con, final String selectQ) throws SQLException
    {
        PreparedStatement statement = con.prepareStatement(selectQ);
        return statement.executeQuery();
    }

    /** executes an prepared statement */
    private static void executeNoResults(final Connection con, final String cql) throws SQLException
    {
        PreparedStatement statement = con.prepareStatement(cql);
        statement.execute();
    }
}
