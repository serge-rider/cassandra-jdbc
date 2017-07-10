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

import java.math.BigDecimal;
import java.net.URL;
import java.sql.*;
import java.util.Calendar;

import static org.jkiss.jdbc.cassandra.ErrorMessages.NO_RESULTSET;
import static org.jkiss.jdbc.cassandra.ErrorMessages.NO_UPDATE_COUNT;

public abstract class CassandraPreparedStatement extends CassandraStatement implements PreparedStatement {

    protected final String keyspace;
    protected final String columnFamily;

    CassandraPreparedStatement(CassandraConnection con, String cql) throws SQLException
    {
        super(con, cql);

        keyspace = CassandraUtils.determineCurrentKeyspace(cql, con.getCurrentKeyspace());
        columnFamily = CassandraUtils.determineCurrentColumnFamily(cql);
    }

    protected abstract void doExecute() throws SQLException;

    public void addBatch() throws SQLException
    {
        throw new SQLFeatureNotSupportedException();
    }


    public void clearParameters() throws SQLException
    {
        checkNotClosed();
    }


    public boolean execute() throws SQLException
    {
        checkNotClosed();
        doExecute();
        return !(currentResultSet == null);
    }


    public ResultSet executeQuery() throws SQLException
    {
        checkNotClosed();
        doExecute();
        if (currentResultSet == null) throw new SQLNonTransientException(NO_RESULTSET);
        return currentResultSet;
    }

    public int executeUpdate() throws SQLException
    {
        checkNotClosed();
        doExecute();
        if (currentResultSet != null) throw new SQLNonTransientException(NO_UPDATE_COUNT);
        return updateCount;
    }

    public ResultSetMetaData getMetaData() throws SQLException
    {
        if (currentResultSet == null) {
            throw new SQLException("No resultset");
        }
        return currentResultSet.getMetaData();
    }

    public ParameterMetaData getParameterMetaData() throws SQLException
    {
        throw new SQLFeatureNotSupportedException();
    }


    public void setBigDecimal(int parameterIndex, BigDecimal decimal) throws SQLException
    {
        throw new SQLFeatureNotSupportedException();
    }


    public void setBoolean(int parameterIndex, boolean truth) throws SQLException
    {
        throw new SQLFeatureNotSupportedException();
    }


    public void setByte(int parameterIndex, byte b) throws SQLException
    {
        throw new SQLFeatureNotSupportedException();
    }


    public void setBytes(int parameterIndex, byte[] bytes) throws SQLException
    {
        throw new SQLFeatureNotSupportedException();
    }


    public void setDate(int parameterIndex, Date value) throws SQLException
    {
        throw new SQLFeatureNotSupportedException();
    }


    public void setDate(int parameterIndex, Date date, Calendar cal) throws SQLException
    {
        throw new SQLFeatureNotSupportedException();
    }


    public void setDouble(int parameterIndex, double decimal) throws SQLException
    {
        throw new SQLFeatureNotSupportedException();
    }


    public void setFloat(int parameterIndex, float decimal) throws SQLException
    {
        throw new SQLFeatureNotSupportedException();
    }


    public void setInt(int parameterIndex, int integer) throws SQLException
    {
        throw new SQLFeatureNotSupportedException();
    }


    public void setLong(int parameterIndex, long bigint) throws SQLException
    {
        throw new SQLFeatureNotSupportedException();
    }


    public void setNString(int parameterIndex, String value) throws SQLException
    {
        throw new SQLFeatureNotSupportedException();
    }


    public void setNull(int parameterIndex, int sqlType) throws SQLException
    {
        throw new SQLFeatureNotSupportedException();
    }


    public void setNull(int parameterIndex, int sqlType, String typeName) throws SQLException
    {
        throw new SQLFeatureNotSupportedException();
    }


    public void setObject(int parameterIndex, Object object) throws SQLException
    {
        throw new SQLFeatureNotSupportedException();
    }

    public void setObject(int parameterIndex, Object object, int targetSqlType) throws SQLException
    {
        throw new SQLFeatureNotSupportedException();
    }

    public void setObject(int parameterIndex, Object object, int targetSqlType, int scaleOrLength) throws SQLException
    {
        throw new SQLFeatureNotSupportedException();
    }

    public void setRowId(int parameterIndex, RowId value) throws SQLException
    {
        throw new SQLFeatureNotSupportedException();
    }


    public void setShort(int parameterIndex, short smallint) throws SQLException
    {
        throw new SQLFeatureNotSupportedException();
    }


    public void setString(int parameterIndex, String value) throws SQLException
    {
        throw new SQLFeatureNotSupportedException();
    }


    public void setTime(int parameterIndex, Time value) throws SQLException
    {
        throw new SQLFeatureNotSupportedException();
    }


    public void setTime(int parameterIndex, Time value, Calendar cal) throws SQLException
    {
        throw new SQLFeatureNotSupportedException();
    }


    public void setTimestamp(int parameterIndex, Timestamp value) throws SQLException
    {
        throw new SQLFeatureNotSupportedException();
    }


    public void setTimestamp(int parameterIndex, Timestamp value, Calendar cal) throws SQLException
    {
        throw new SQLFeatureNotSupportedException();
    }


    public void setURL(int parameterIndex, URL value) throws SQLException
    {
        throw new SQLFeatureNotSupportedException();
    }

}
