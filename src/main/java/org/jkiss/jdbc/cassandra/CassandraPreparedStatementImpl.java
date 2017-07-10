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

import org.apache.cassandra.thrift.*;
import org.apache.thrift.TException;
import org.jkiss.jdbc.cassandra.types.*;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.URL;
import java.nio.ByteBuffer;
import java.sql.Date;
import java.sql.*;
import java.util.*;

public class CassandraPreparedStatementImpl extends CassandraPreparedStatement {

    private int itemId = -1;

    /**
     * a Map of the current bound values encountered in setXXX methods
     */
    private Map<Integer, ByteBuffer> bindValues = new LinkedHashMap<Integer, ByteBuffer>();


    CassandraPreparedStatementImpl(CassandraConnection con, String cql) throws SQLException
    {
        super(con, cql);
    }

    private void checkIndex(int index) throws SQLException
    {
        if (index < 1)
            throw new SQLRecoverableException(
                String.format("the column index must be a positive number : %d", index));
    }

    private List<ByteBuffer> getBindValues() throws SQLException
    {
        List<ByteBuffer> values = new ArrayList<ByteBuffer>();
        for (int i = 1; i <= bindValues.size(); i++) {
            ByteBuffer value = bindValues.get(i);
            if (value == null) {
                throw new SQLRecoverableException(String.format("the bound value for index: %d was not set", i));
            }
            values.add(value);
        }
        return values;
    }

    protected void doExecute() throws SQLException
    {
        if (itemId == -1) {
            try {
                CqlPreparedResult result = prepare(cql);

                this.itemId = result.itemId;
                int bindVariableCount = result.count;
                if (bindValues.size() != bindVariableCount) {
                    throw new SQLRecoverableException(
                        "CQL [" + cql + "] requires exactly " + bindVariableCount + " variables while only " + bindValues.size() + " specified");
                }

            } catch (InvalidRequestException e) {
                throw new SQLSyntaxErrorException(e);
            } catch (TException e) {
                throw new SQLNonTransientConnectionException(e);
            }
        }
        try {
            resetResults();
            CqlResult result = execute(itemId, getBindValues());

            switch (result.getType()) {
                case ROWS:
                    currentResultSet = new CassandraResultSet(this, result, keyspace, columnFamily);
                    break;
                case INT:
                    updateCount = result.getNum();
                    break;
                case VOID:
                    updateCount = 0;
                    break;
            }
        } catch (InvalidRequestException e) {
            throw new SQLSyntaxErrorException(e.getWhy(), e);
        } catch (UnavailableException e) {
            throw new SQLNonTransientConnectionException(ErrorMessages.NO_SERVER, e);
        } catch (TimedOutException e) {
            throw new SQLTransientConnectionException(e.getMessage());
        } catch (SchemaDisagreementException e) {
            throw new SQLRecoverableException(ErrorMessages.SCHEMA_MISMATCH, e);
        } catch (TException e) {
            throw new SQLNonTransientConnectionException(e.getMessage(), e);
        }
    }

    public void clearParameters() throws SQLException
    {
        checkNotClosed();
        bindValues.clear();
    }

    public void setBigDecimal(int parameterIndex, BigDecimal decimal) throws SQLException
    {
        checkNotClosed();
        checkIndex(parameterIndex);
        bindValues.put(parameterIndex, JdbcDecimal.instance.decompose(decimal));
    }


    public void setBoolean(int parameterIndex, boolean truth) throws SQLException
    {
        checkNotClosed();
        checkIndex(parameterIndex);
        bindValues.put(parameterIndex, JdbcBoolean.instance.decompose(truth));
    }


    public void setByte(int parameterIndex, byte b) throws SQLException
    {
        checkNotClosed();
        checkIndex(parameterIndex);
        bindValues.put(parameterIndex, JdbcInteger.instance.decompose(BigInteger.valueOf(b)));
    }


    public void setBytes(int parameterIndex, byte[] bytes) throws SQLException
    {
        checkNotClosed();
        checkIndex(parameterIndex);
        bindValues.put(parameterIndex, ByteBuffer.wrap(bytes));
    }


    public void setDate(int parameterIndex, Date value) throws SQLException
    {
        checkNotClosed();
        checkIndex(parameterIndex);
        // date type data is handled as an 8 byte Long value of milliseconds since the epoch (handled in decompose() )
        bindValues.put(parameterIndex, JdbcDate.instance.decompose(value));
    }


    public void setDate(int parameterIndex, Date date, Calendar cal) throws SQLException
    {
        // silently ignore the calendar argument it is not useful for the Cassandra implementation
        setDate(parameterIndex, date);
    }


    public void setDouble(int parameterIndex, double decimal) throws SQLException
    {
        checkNotClosed();
        checkIndex(parameterIndex);
        bindValues.put(parameterIndex, JdbcDouble.instance.decompose(decimal));
    }


    public void setFloat(int parameterIndex, float decimal) throws SQLException
    {
        checkNotClosed();
        checkIndex(parameterIndex);
        bindValues.put(parameterIndex, JdbcFloat.instance.decompose(decimal));
    }


    public void setInt(int parameterIndex, int integer) throws SQLException
    {
        checkNotClosed();
        checkIndex(parameterIndex);
        bindValues.put(parameterIndex, JdbcInt32.instance.decompose(integer));
    }


    public void setLong(int parameterIndex, long bigint) throws SQLException
    {
        checkNotClosed();
        checkIndex(parameterIndex);
        bindValues.put(parameterIndex, JdbcLong.instance.decompose(bigint));
    }


    public void setNString(int parameterIndex, String value) throws SQLException
    {
        // treat like a String
        setString(parameterIndex, value);
    }


    public void setNull(int parameterIndex, int sqlType) throws SQLException
    {
        checkNotClosed();
        checkIndex(parameterIndex);
        // silently ignore type for cassandra... just store an empty String
        bindValues.put(parameterIndex, CassandraUtils.EMPTY_BYTE_BUFFER);
    }


    public void setNull(int parameterIndex, int sqlType, String typeName) throws SQLException
    {
        // silently ignore type and type name for cassandra... just store an empty BB
        setNull(parameterIndex, sqlType);
    }


    public void setObject(int parameterIndex, Object object) throws SQLException
    {
        // For now all objects are forced to String type
        setObject(parameterIndex, object, Types.VARCHAR, 0);
    }

    public void setObject(int parameterIndex, Object object, int targetSqlType) throws SQLException
    {
        setObject(parameterIndex, object, targetSqlType, 0);
    }

    public void setObject(int parameterIndex, Object object, int targetSqlType, int scaleOrLength) throws SQLException
    {
        checkNotClosed();
        checkIndex(parameterIndex);

        ByteBuffer variable = HandleObjects.makeBytes(object, targetSqlType, scaleOrLength);

        if (variable == null) throw new SQLNonTransientException("Problem mapping object to JDBC Type");

        bindValues.put(parameterIndex, variable);
    }

    public void setRowId(int parameterIndex, RowId value) throws SQLException
    {
        checkNotClosed();
        checkIndex(parameterIndex);
        bindValues.put(parameterIndex, ByteBuffer.wrap(value.getBytes()));
    }


    public void setShort(int parameterIndex, short smallint) throws SQLException
    {
        checkNotClosed();
        checkIndex(parameterIndex);
        bindValues.put(parameterIndex, JdbcInteger.instance.decompose(BigInteger.valueOf(smallint)));
    }


    public void setString(int parameterIndex, String value) throws SQLException
    {
        checkNotClosed();
        checkIndex(parameterIndex);
        bindValues.put(parameterIndex, CassandraUtils.bytes(value));
    }


    public void setTime(int parameterIndex, Time value) throws SQLException
    {
        checkNotClosed();
        checkIndex(parameterIndex);
        // time type data is handled as an 8 byte Long value of milliseconds since the epoch
        bindValues.put(parameterIndex, JdbcLong.instance.decompose(value.getTime()));
    }


    public void setTime(int parameterIndex, Time value, Calendar cal) throws SQLException
    {
        // silently ignore the calendar argument it is not useful for the Cassandra implementation
        setTime(parameterIndex, value);
    }


    public void setTimestamp(int parameterIndex, Timestamp value) throws SQLException
    {
        checkNotClosed();
        checkIndex(parameterIndex);
        // timestamp type data is handled as an 8 byte Long value of milliseconds since the epoch. Nanos are not supported and are ignored
        bindValues.put(parameterIndex, JdbcLong.instance.decompose(value.getTime()));
    }


    public void setTimestamp(int parameterIndex, Timestamp value, Calendar cal) throws SQLException
    {
        // silently ignore the calendar argument it is not useful for the Cassandra implementation
        setTimestamp(parameterIndex, value);
    }


    public void setURL(int parameterIndex, URL value) throws SQLException
    {
        checkNotClosed();
        checkIndex(parameterIndex);
        // URl type data is handled as an string
        String url = value.toString();
        bindValues.put(parameterIndex, CassandraUtils.bytes(url));
    }

    protected CqlResult execute(int itemId, List<ByteBuffer> values)
        throws InvalidRequestException, UnavailableException, TimedOutException, SchemaDisagreementException, TException
    {
        return connection.getClient().execute_prepared_cql_query(itemId, values);
    }

    protected CqlPreparedResult prepare(String queryStr, Compression compression) throws InvalidRequestException, TException
    {
        queryStr = CassandraUtils.modifyQueryLimits(queryStr, maxRows);
        return connection.getClient().prepare_cql_query(CassandraUtils.compressQuery(queryStr, compression), compression);
    }

    protected CqlPreparedResult prepare(String queryStr) throws InvalidRequestException, TException
    {
        return prepare(queryStr, CassandraConnection.defaultCompression);
    }

}
