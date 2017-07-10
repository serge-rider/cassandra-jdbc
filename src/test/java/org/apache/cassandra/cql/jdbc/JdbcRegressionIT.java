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
package org.apache.cassandra.cql.jdbc;

import static org.junit.Assert.*;

import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;

import org.apache.cassandra.cql.ConnectionDetails;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

public class JdbcRegressionIT
{
    private static final String HOST = System.getProperty("host", ConnectionDetails.getHost());
    private static final int PORT = Integer.parseInt(System.getProperty("port", ConnectionDetails.getPort()+""));
    private static final String KEYSPACE = "TestKS";
    private static final String CQLV3 = "3.0.0";
      
    private static java.sql.Connection con = null;
    

    @BeforeClass
    public static void setUpBeforeClass() throws Exception
    {
        Class.forName("org.apache.cassandra.cql.jdbc.CassandraDriver");
        String URL = String.format("jdbc:jkiss:cassandra://%s:%d/%s",HOST,PORT,"system");
        System.out.println("Connection URL = '"+URL +"'");
        
        con = DriverManager.getConnection(URL);
        Statement stmt = con.createStatement();
        
        // Drop Keyspace
        String dropKS = String.format("DROP KEYSPACE %s;",KEYSPACE);
        
        try { stmt.execute(dropKS);}
        catch (Exception e){/* Exception on DROP is OK */}

        // Create KeySpace
        String createKS = String.format("CREATE KEYSPACE %s WITH strategy_class = SimpleStrategy AND strategy_options:replication_factor = 1;",KEYSPACE);
        System.out.println("createKS = '"+createKS+"'");
        stmt = con.createStatement();
        stmt.execute("USE system;");
        stmt.execute(createKS);
        
        // Use Keyspace
        String useKS = String.format("USE %s;",KEYSPACE);
        stmt.execute(useKS);
        
        // Create the target Column family
        String createCF = "CREATE COLUMNFAMILY RegressionTest (keyname text PRIMARY KEY," 
                        + "bValue boolean, "
                        + "iValue int "
                        + ") WITH comparator = ascii AND default_validation = bigint;";
        
        
        stmt.execute(createCF);
        stmt.close();
        con.close();

        // open it up again to see the new CF
        con = DriverManager.getConnection(String.format("jdbc:jkiss:cassandra://%s:%d/%s",HOST,PORT,KEYSPACE));
        System.out.println(con);

    }
    
    @AfterClass
    public static void tearDownAfterClass() throws Exception
    {
        if (con!=null) con.close();
    }


//    @Ignore
    @Test
    public void testIssue10() throws Exception
    {
        String insert = "INSERT INTO RegressionTest (keyname,bValue,iValue) VALUES( 'key0',true, 2000);";
        Statement statement = con.createStatement();

        statement.executeUpdate(insert);
        statement.close();
        
        statement = con.createStatement();
        ResultSet result = statement.executeQuery("SELECT bValue,notThere,iValue FROM RegressionTest WHERE keyname=key0;");
        result.next();
        
        boolean b = result.getBoolean(1);
        assertTrue(b);
        
        long l = result.getLong("notThere");
        assertEquals(0,l);
        
        int i = result.getInt(3);
        assertEquals(2000, i);
   }

    @Ignore
    @Test
    public void testIssue15() throws Exception
    {
//        con.close();
//
//        con = DriverManager.getConnection(String.format("jdbc:jkiss:cassandra://%s:%d/%s?version=%s",HOST,PORT,KEYSPACE,CQLV3));
//        System.out.println(con);
//        con.close();

//        con = DriverManager.getConnection(String.format("jdbc:jkiss:cassandra://%s:%d/%s",HOST,PORT,KEYSPACE));
//        System.out.println(con);
//        con.close();

    }
    
//    @Ignore
    @Test
    public void testIssue18() throws Exception
    {
       Statement statement = con.createStatement();

       String truncate = "TRUNCATE RegressionTest;";
       statement.execute(truncate);
       
       String insert1 = "INSERT INTO RegressionTest (keyname,bValue,iValue) VALUES( 'key0',true, 2000);";
       statement.executeUpdate(insert1);
       
       String insert2 = "INSERT INTO RegressionTest (keyname,bValue) VALUES( 'key1',false);";
       statement.executeUpdate(insert2);
       
       
       
       String select = "SELECT * from RegressionTest;";
       
       ResultSet result = statement.executeQuery(select);
       
       ResultSetMetaData metadata = result.getMetaData();
       
       int colCount = metadata.getColumnCount();
       
       System.out.println("Before doing a next()");
       System.out.printf("(%d) ",result.getRow());
       for (int i = 1; i <= colCount; i++)
       {
           System.out.print(showColumn(i,result)+ " "); 
       }
       System.out.println();
       
       
       System.out.println("Fetching each row with a next()");
       while (result.next())
       {
           metadata = result.getMetaData();
           colCount = metadata.getColumnCount();
           System.out.printf("(%d) ",result.getRow());
           for (int i = 1; i <= colCount; i++)
           {
               System.out.print(showColumn(i,result)+ " "); 
           }
           System.out.println();
       }
    }
    
//    @Ignore
    @Test
    public void testIssue33() throws Exception
    {
        Statement stmt = con.createStatement();
        
        // Create the target Column family
        String createCF = "CREATE COLUMNFAMILY t33 (k int PRIMARY KEY," 
                        + "c text "
                        + ") WITH comparator = ascii AND default_validation = text;";        
        
        stmt.execute(createCF);
        stmt.close();
        con.close();

        // open it up again to see the new CF
        con = DriverManager.getConnection(String.format("jdbc:jkiss:cassandra://%s:%d/%s",HOST,PORT,KEYSPACE));
        
        // paraphrase of the snippet from the ISSUE #33 provided test
        PreparedStatement statement = con.prepareStatement("update t33 set c=? where k=123");
        statement.setString(1, "mark");
        statement.executeUpdate();

        ResultSet result = statement.executeQuery("SELECT * FROM t33;");
        
        ResultSetMetaData metadata = result.getMetaData();
        
        int colCount = metadata.getColumnCount();
        
        System.out.println("TestMetadata Issue #33");
        System.out.println("--------------");
        while (result.next())
        {
            metadata = result.getMetaData();
            colCount = metadata.getColumnCount();
            System.out.printf("(%d) ",result.getRow());
            for (int i = 1; i <= colCount; i++)
            {
                System.out.print(showColumn(i,result)+ " "); 
            }
            System.out.println();
        }
   }
    
    @Test
    public void testIssue46() throws Exception
    {
        Statement stmt = con.createStatement();
        
        // Create the target Column family
        String createCF = "CREATE COLUMNFAMILY t46 (k text PRIMARY KEY," 
                        + "t text, "
                        + "i int "
                        + ") WITH comparator = ascii AND default_validation = text;";        
        
        stmt.execute(createCF);
        stmt.close();
        con.close();

        // open it up again to see the new CF
        con = DriverManager.getConnection(String.format("jdbc:jkiss:cassandra://%s:%d/%s",HOST,PORT,KEYSPACE));
        stmt = con.createStatement();

        String insert1 = "INSERT INTO t46 (k,t,i) VALUES( 'key0','Just Text', 2000);";
        
        stmt.executeUpdate(insert1);
        
        String insert2 = "INSERT INTO t46 (k,t,i) VALUES(?,?,? );";
        
        PreparedStatement pstmt = con.prepareStatement(insert2);
        pstmt.setString(1, "key1");
        pstmt.setString(2, "Some More Text");
        pstmt.setNull(3, Types.INTEGER);
        pstmt.execute();
        
        ResultSet result = stmt.executeQuery("SELECT * FROM t46;");
        
        ResultSetMetaData metadata = result.getMetaData();
        
        int colCount = metadata.getColumnCount();
        
        System.out.println("TestMetadata Issue #46");
        System.out.println("--------------");
        while (result.next())
        {
            metadata = result.getMetaData();
            colCount = metadata.getColumnCount();
            System.out.printf("(%d) ",result.getRow());
            for (int i = 1; i <= colCount; i++)
            {
                System.out.print(showColumn(i,result)+ " "); 
            }
            System.out.println();
        }

        
    } 
    
    
    private final String  showColumn(int index, ResultSet result) throws SQLException
    {
        StringBuilder sb = new StringBuilder();
        sb.append("[").append(index).append("]");
        sb.append(result.getObject(index));
        return sb.toString();
    }
}
