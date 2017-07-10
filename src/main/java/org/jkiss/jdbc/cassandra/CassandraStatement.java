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

import java.sql.*;

/**
 * Cassandra statement
 */
class CassandraStatement extends AbstractStatement implements Comparable<Object> {
    /**
     * The connection.
     */
    protected CassandraConnection connection;

    /**
     * The cql.
     */
    protected String cql;

    protected int fetchDirection = ResultSet.FETCH_FORWARD;

    protected int fetchSize = 0;

    protected int maxFieldSize = 0;

    protected int maxRows = 0;

    protected int resultSetType = CassandraResultSet.DEFAULT_TYPE;

    protected int resultSetConcurrency = CassandraResultSet.DEFAULT_CONCURRENCY;

    protected int resultSetHoldability = CassandraResultSet.DEFAULT_HOLDABILITY;

    protected ResultSet currentResultSet = null;

    protected int updateCount = -1;

    protected boolean escapeProcessing = true;

    CassandraStatement(CassandraConnection con) throws SQLException
    {
        this(con, null);
    }

    CassandraStatement(CassandraConnection con, String cql) throws SQLException
    {
        this.connection = con;
        this.cql = cql;
    }

    CassandraStatement(CassandraConnection con, String cql, int resultSetType, int resultSetConcurrency) throws SQLException
    {
        this(con, cql, resultSetType, resultSetConcurrency, ResultSet.HOLD_CURSORS_OVER_COMMIT);
    }

    CassandraStatement(CassandraConnection con, String cql, int resultSetType, int resultSetConcurrency,
                       int resultSetHoldability) throws SQLException
    {
        this.connection = con;
        this.cql = cql;

        if (!(resultSetType == ResultSet.TYPE_FORWARD_ONLY
            || resultSetType == ResultSet.TYPE_SCROLL_INSENSITIVE
            || resultSetType == ResultSet.TYPE_SCROLL_SENSITIVE))
            throw new SQLSyntaxErrorException(ErrorMessages.BAD_TYPE_RSET);
        this.resultSetType = resultSetType;

        if (!(resultSetConcurrency == ResultSet.CONCUR_READ_ONLY
            || resultSetConcurrency == ResultSet.CONCUR_UPDATABLE))
            throw new SQLSyntaxErrorException(ErrorMessages.BAD_TYPE_RSET);
        this.resultSetConcurrency = resultSetConcurrency;


        if (!(resultSetHoldability == ResultSet.HOLD_CURSORS_OVER_COMMIT
            || resultSetHoldability == ResultSet.CLOSE_CURSORS_AT_COMMIT))
            throw new SQLSyntaxErrorException(ErrorMessages.BAD_HOLD_RSET);
        this.resultSetHoldability = resultSetHoldability;
    }

    public String getCql()
    {
        return cql;
    }

    public void addBatch(String arg0) throws SQLException
    {
        checkNotClosed();
        throw new SQLFeatureNotSupportedException(ErrorMessages.NO_BATCH);
    }

    protected final void checkNotClosed() throws SQLException
    {
        if (isClosed()) throw new SQLRecoverableException(ErrorMessages.WAS_CLOSED_STMT);
    }

    public void clearBatch() throws SQLException
    {
        checkNotClosed();
        throw new SQLFeatureNotSupportedException(ErrorMessages.NO_BATCH);
    }

    public void clearWarnings() throws SQLException
    {
        // This implementation does not support the collection of warnings so clearing is a no-op
        // but it is still an exception to call this on a closed connection.
        checkNotClosed();
    }

    public void close() throws SQLException
    {
        if (connection != null) {
            connection.removeStatement(this);
        }
        connection = null;
        cql = null;
    }

    private void executeCQL(String sql) throws SQLException
    {
        try {
            this.cql = sql;

            resetResults();
            CqlResult rSet = connection.executeCQL(sql, maxRows);
            String keyspace = connection.getCurrentKeyspace();

            switch (rSet.getType()) {
                case ROWS:
                    currentResultSet = new CassandraResultSet(
                        this,
                        rSet,
                        CassandraUtils.determineCurrentKeyspace(sql, keyspace),
                        CassandraUtils.determineCurrentColumnFamily(sql));
                    break;
                case INT:
                    updateCount = rSet.getNum();
                    break;
                case VOID:
                    updateCount = 0;
                    break;
            }
        } catch (InvalidRequestException e) {
            throw new SQLSyntaxErrorException(e.getWhy() + "\n'" + sql + "'", e);
        } catch (UnavailableException e) {
            throw new SQLNonTransientConnectionException(ErrorMessages.NO_SERVER, e);
        } catch (TimedOutException e) {
            throw new SQLTransientConnectionException(e);
        } catch (SchemaDisagreementException e) {
            throw new SQLRecoverableException(ErrorMessages.SCHEMA_MISMATCH);
        } catch (TException e) {
            throw new SQLNonTransientConnectionException(e);
        }

    }

    public boolean execute(String query) throws SQLException
    {
        checkNotClosed();
        executeCQL(query);
        return !(currentResultSet == null);
    }

    public boolean execute(String sql, int autoGeneratedKeys) throws SQLException
    {
        checkNotClosed();

        if (!(autoGeneratedKeys == RETURN_GENERATED_KEYS || autoGeneratedKeys == NO_GENERATED_KEYS))
            throw new SQLSyntaxErrorException(ErrorMessages.BAD_AUTO_GEN);

        if (autoGeneratedKeys == RETURN_GENERATED_KEYS)
            throw new SQLFeatureNotSupportedException(ErrorMessages.NO_GEN_KEYS);

        return execute(sql);
    }

    @Override
    public boolean execute(String sql, int[] columnIndexes) throws SQLException
    {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public boolean execute(String sql, String[] columnNames) throws SQLException
    {
        throw new SQLFeatureNotSupportedException();
    }

    public int[] executeBatch() throws SQLException
    {
        throw new SQLFeatureNotSupportedException(ErrorMessages.NO_BATCH);
    }

    public ResultSet executeQuery(String query) throws SQLException
    {
        checkNotClosed();
        executeCQL(query);
        if (currentResultSet == null)
            throw new SQLNonTransientException(ErrorMessages.NO_RESULTSET);
        return currentResultSet;
    }

    public int executeUpdate(String query) throws SQLException
    {
        checkNotClosed();
        executeCQL(query);
        if (currentResultSet != null)
            throw new SQLNonTransientException(ErrorMessages.NO_UPDATE_COUNT);
        return updateCount;
    }

    public int executeUpdate(String sql, int autoGeneratedKeys) throws SQLException
    {
        checkNotClosed();

        if (!(autoGeneratedKeys == RETURN_GENERATED_KEYS || autoGeneratedKeys == NO_GENERATED_KEYS))
            throw new SQLFeatureNotSupportedException(ErrorMessages.BAD_AUTO_GEN);

        return executeUpdate(sql);
    }

    @Override
    public int executeUpdate(String sql, int[] columnIndexes) throws SQLException
    {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public int executeUpdate(String sql, String[] columnNames) throws SQLException
    {
        throw new SQLFeatureNotSupportedException();
    }

    public CassandraConnection getConnection()
    {
        return connection;
    }

    public int getFetchDirection()
    {
        return fetchDirection;
    }

    public int getFetchSize()
    {
        return fetchSize;
    }

    public int getMaxFieldSize()
    {
        return maxFieldSize;
    }

    public int getMaxRows()
    {
        return maxRows;
    }

    public boolean getMoreResults() throws SQLException
    {
        checkNotClosed();
        resetResults();
        // in the current Cassandra implementation there are never MORE results
        return false;
    }

    public boolean getMoreResults(int current) throws SQLException
    {
        checkNotClosed();

        switch (current) {
            case CLOSE_CURRENT_RESULT:
                resetResults();
                break;

            case CLOSE_ALL_RESULTS:
            case KEEP_CURRENT_RESULT:
                throw new SQLFeatureNotSupportedException(ErrorMessages.NO_MULTIPLE);

            default:
                throw new SQLSyntaxErrorException(String.format(ErrorMessages.BAD_KEEP_RSET, current));
        }
        // in the current Cassandra implementation there are never MORE results
        return false;
    }

    public int getQueryTimeout() throws SQLException
    {
        // the Cassandra implementation does not support timeouts on queries
        return 0;
    }

    public ResultSet getResultSet() throws SQLException
    {
        checkNotClosed();
        return currentResultSet;
    }

    public int getResultSetConcurrency() throws SQLException
    {
        checkNotClosed();
        return ResultSet.CONCUR_READ_ONLY;
    }

    public int getResultSetHoldability() throws SQLException
    {
        checkNotClosed();
        // the Cassandra implementations does not support commits so this is the closest match
        return ResultSet.HOLD_CURSORS_OVER_COMMIT;
    }

    public int getResultSetType() throws SQLException
    {
        checkNotClosed();
        return ResultSet.TYPE_FORWARD_ONLY;
    }

    public int getUpdateCount() throws SQLException
    {
        checkNotClosed();
        return updateCount;
    }

    public SQLWarning getWarnings() throws SQLException
    {
        checkNotClosed();
        return null;
    }

    public boolean isClosed() throws SQLException
    {
        return connection == null;
    }

    public boolean isPoolable() throws SQLException
    {
        checkNotClosed();
        return false;
    }

    public boolean isWrapperFor(Class<?> iface) throws SQLException
    {
        return false;
    }

    protected final void resetResults()
    {
        currentResultSet = null;
        updateCount = -1;
    }

    public void setEscapeProcessing(boolean enable) throws SQLException
    {
        checkNotClosed();
        // the Cassandra implementation does not currently look at this
        escapeProcessing = enable;
    }

    public void setFetchDirection(int direction) throws SQLException
    {
        checkNotClosed();

        if (direction == ResultSet.FETCH_FORWARD || direction == ResultSet.FETCH_REVERSE || direction == ResultSet.FETCH_UNKNOWN) {
            if ((getResultSetType() == ResultSet.TYPE_FORWARD_ONLY) && (direction != ResultSet.FETCH_FORWARD))
                throw new SQLSyntaxErrorException(String.format(ErrorMessages.BAD_FETCH_DIR, direction));
            fetchDirection = direction;
        } else throw new SQLSyntaxErrorException(String.format(ErrorMessages.BAD_FETCH_DIR, direction));
    }


    public void setFetchSize(int size) throws SQLException
    {
        checkNotClosed();
        if (size < 0) throw new SQLSyntaxErrorException(String.format(ErrorMessages.BAD_FETCH_SIZE, size));
        fetchSize = size;
    }

    public void setMaxFieldSize(int arg0) throws SQLException
    {
        checkNotClosed();
        // silently ignore this setting. always use default 0 (unlimited)
    }

    public void setMaxRows(int maxRows) throws SQLException
    {
        checkNotClosed();
        // silently ignore this setting. always use default 0 (unlimited)
        this.maxRows = maxRows;
    }

    public void setPoolable(boolean poolable) throws SQLException
    {
        checkNotClosed();
        // silently ignore any attempt to set this away from the current default (false)
    }

    public void setQueryTimeout(int arg0) throws SQLException
    {
        checkNotClosed();
        // silently ignore any attempt to set this away from the current default (0)
    }

    public <T> T unwrap(Class<T> iface) throws SQLException
    {
        throw new SQLFeatureNotSupportedException("Can't unwrap " + iface.getName());
    }

    public int compareTo(Object target)
    {
        if (this.equals(target)) return 0;
        if (this.hashCode() < target.hashCode()) return -1;
        else return 1;
    }
}
