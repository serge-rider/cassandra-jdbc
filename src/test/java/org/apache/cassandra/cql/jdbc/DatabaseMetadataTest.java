/*
 * 
 * Licensed to the Apache Software Foundation (ASF) under one;
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

import static org.junit.Assert.fail;

import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class DatabaseMetadataTest
{
	/**
	 * Cassandra-unit starts on localhost:9171
	 */
    private static final String HOST = "localhost";
    private static final int PORT = 9171;
    private static final String KEYSPACE = "TestKS";
    
    private java.sql.Connection con = null;
    

    @Before
    public void setUpBeforeClass() throws Exception
    {
        Class.forName("org.apache.cassandra.cql.jdbc.CassandraDriver");
        con = DriverManager.getConnection(String.format("jdbc:jkiss:cassandra://%s:%d/%s",HOST,PORT,"system"));
        Statement stmt = con.createStatement();

        // Drop Keyspace
        String dropKS = String.format("DROP KEYSPACE %s;",KEYSPACE);
        
        try { stmt.execute(dropKS);}
        catch (Exception e){/* Exception on DROP is OK */}
        
        // Create KeySpace
        String createKS = String.format("CREATE KEYSPACE %s WITH strategy_class = SimpleStrategy AND strategy_options:replication_factor = 1;",KEYSPACE);
        stmt = con.createStatement();
        stmt.execute(createKS);
        
        // Use Keyspace
        String useKS = String.format("USE %s;",KEYSPACE);
        stmt.execute(useKS);
        
               
        // Create the target Column family
        String create = "CREATE COLUMNFAMILY Test (KEY text PRIMARY KEY) WITH comparator = ascii AND default_validation = bigint;";
        stmt = con.createStatement();
        stmt.execute(create);
        stmt.close();
        con.close();

        // open it up again to see the new CF
        con = DriverManager.getConnection(String.format("jdbc:jkiss:cassandra://%s:%d/%s",HOST,PORT,KEYSPACE));
    }
    

    @After
    public void tearDownAfterClass() throws Exception
    {
        if (con!=null) con.close();
    }


    @Test
    public void testGetTablesViaDatabaseMetadata() throws Exception
    {
    	ResultSet rs = con.getMetaData().getTables(null, null, null, null);
    	int resultCount = 0;
    	if(!rs.next()) {
    		fail("Did not get any result set for getMetadata().getTables()");
    	}
    }

}
