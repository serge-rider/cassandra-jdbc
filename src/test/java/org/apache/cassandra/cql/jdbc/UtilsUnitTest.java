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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import java.util.Properties;

import org.jkiss.jdbc.cassandra.CassandraConstants;
import org.jkiss.jdbc.cassandra.CassandraUtils;
import org.junit.BeforeClass;
import org.junit.Test;

public class UtilsUnitTest
{

    @BeforeClass
    public static void setUpBeforeClass() throws Exception
    {}

    @Test
    public void testParseURL() throws Exception
    {
        String happypath = "jdbc:jkiss:cassandra://localhost:9170/Keyspace1?version=2.0.0";
        Properties props = CassandraUtils.parseURL(happypath);
        assertEquals("localhost", props.getProperty(CassandraConstants.PROP_SERVER_NAME));
        assertEquals("9170", props.getProperty(CassandraConstants.PROP_PORT_NUMBER));
        assertEquals("Keyspace1", props.getProperty(CassandraConstants.PROP_DATABASE_NAME));
        assertEquals("2.0.0", props.getProperty(CassandraConstants.PROP_CQL_VERSION));
        
        String noport = "jdbc:jkiss:cassandra://localhost/Keyspace1?version=2.0.0";
        props = CassandraUtils.parseURL(noport);
        assertEquals("localhost", props.getProperty(CassandraConstants.PROP_SERVER_NAME));
        assertEquals("9160", props.getProperty(CassandraConstants.PROP_PORT_NUMBER));
        assertEquals("Keyspace1", props.getProperty(CassandraConstants.PROP_DATABASE_NAME));
        assertEquals("2.0.0", props.getProperty(CassandraConstants.PROP_CQL_VERSION));
        
        String noversion = "jdbc:jkiss:cassandra://localhost:9170/Keyspace1";
        props = CassandraUtils.parseURL(noversion);
        assertEquals("localhost", props.getProperty(CassandraConstants.PROP_SERVER_NAME));
        assertEquals("9170", props.getProperty(CassandraConstants.PROP_PORT_NUMBER));
        assertEquals("Keyspace1", props.getProperty(CassandraConstants.PROP_DATABASE_NAME));
        assertNull(props.getProperty(CassandraConstants.PROP_CQL_VERSION));
        
        String nokeyspaceonly = "jdbc:jkiss:cassandra://localhost:9170?version=2.0.0";
        props = CassandraUtils.parseURL(nokeyspaceonly);
        assertEquals("localhost", props.getProperty(CassandraConstants.PROP_SERVER_NAME));
        assertEquals("9170", props.getProperty(CassandraConstants.PROP_PORT_NUMBER));
        assertNull(props.getProperty(CassandraConstants.PROP_DATABASE_NAME));
        assertEquals("2.0.0", props.getProperty(CassandraConstants.PROP_CQL_VERSION));
        
        String nokeyspaceorver = "jdbc:jkiss:cassandra://localhost:9170";
        props = CassandraUtils.parseURL(nokeyspaceorver);
        assertEquals("localhost", props.getProperty(CassandraConstants.PROP_SERVER_NAME));
        assertEquals("9170", props.getProperty(CassandraConstants.PROP_PORT_NUMBER));
        assertNull(props.getProperty(CassandraConstants.PROP_DATABASE_NAME));
        assertNull(props.getProperty(CassandraConstants.PROP_CQL_VERSION));
    }
  
    /*
     * doesn't compile anymore because createSubName() doesn't exist
     
    @TestMetadata
    public void testCreateSubName() throws Exception
    {
        String happypath = "jdbc:jkiss:cassandra://localhost:9170/Keyspace1?version=2.0.0";
        Properties props = CassandraUtils.parseURL(happypath);
        
        String result = CassandraUtils.createSubName(props);
        assertEquals(happypath, CassandraConstants.PROTOCOL+result);
    }
    */
}
