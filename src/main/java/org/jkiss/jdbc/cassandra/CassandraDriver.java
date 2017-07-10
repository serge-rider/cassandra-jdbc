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
package org.jkiss.jdbc.cassandra;

import java.sql.*;
import java.util.Properties;

import static org.jkiss.jdbc.cassandra.CassandraConstants.*;

/**
 * CassandraDriver.
 */
public class CassandraDriver implements Driver {

    static {
        // Register the CassandraDriver with DriverManager
        try {
            CassandraDriver driverInst = new CassandraDriver();
            DriverManager.registerDriver(driverInst);
        } catch (SQLException e) {
            throw new RuntimeException(e.getMessage());
        }
    }

    /**
     * Method to validate whether provided connection url matches with pattern or not.
     */
    public boolean acceptsURL(String url) throws SQLException
    {
        return url.startsWith(PROTOCOL);
    }

    /**
     * Method to return connection instance for given connection url and connection props.
     */
    public Connection connect(String url, Properties props) throws SQLException
    {
        Properties finalProps;
        if (acceptsURL(url)) {
            // parse the URL into a set of Properties
            finalProps = CassandraUtils.parseURL(url);

            // override any matching values in finalProps with values from props
            finalProps.putAll(props);

            return new CassandraConnection(finalProps);
        } else {
            return null; // signal it is the wrong driver for this protocol:subprotocol
        }
    }

    /**
     * Returns default major version.
     */
    public int getMajorVersion()
    {
        return CassandraConstants.DRIVER_MAJOR_VERSION;
    }

    /**
     * Returns default minor version.
     */
    public int getMinorVersion()
    {
        return CassandraConstants.DRIVER_MINOR_VERSION;
    }

    /**
     * Returns default driver property info object.
     */
    public DriverPropertyInfo[] getPropertyInfo(String url, Properties props) throws SQLException
    {
        if (props == null) props = new Properties();

        DriverPropertyInfo[] info = new DriverPropertyInfo[2];

        info[0] = new DriverPropertyInfo(PROP_USER, props.getProperty(PROP_USER));
        info[0].description = "User name";

        info[1] = new DriverPropertyInfo(PROP_PASSWORD, props.getProperty(PROP_PASSWORD));
        info[1].description = "User password";

        return info;
    }

    /**
     * Returns true, if it is jdbc compliant.
     */
    public boolean jdbcCompliant()
    {
        return true;
    }

    public java.util.logging.Logger getParentLogger() throws SQLFeatureNotSupportedException
    {
        throw new SQLFeatureNotSupportedException();
    }
}
