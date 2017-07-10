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
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;

import java.sql.*;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ConcurrentSkipListSet;

/**
 * Implementation class for {@link Connection}.
 */
public class CassandraConnection extends AbstractConnection {

    public static Compression defaultCompression = Compression.GZIP;

    /**
     * Connection Properties
     */
    private Properties connectionProps;

    /**
     * Client Info Properties (currently unused)
     */
    private Properties clientInfo = new Properties();

    /**
     * Set of all Statements that have been created by this connection
     */
    private Set<Statement> statements = new ConcurrentSkipListSet<Statement>();

    private Cassandra.Client client;
    private TTransport transport;

    private String username = null;
    private String url = null;
    private String currentKeyspace;
    private ColumnDecoder decoder;
    private CassandraDatabaseMetaData meta;
    private boolean structResultSet;
    private boolean version11;


    /**
    * Instantiates a new CassandraConnection.
    */
    public CassandraConnection(Properties props) throws SQLException
    {
        connectionProps = (Properties) props.clone();
        clientInfo = new Properties();
        url = CassandraConstants.PROTOCOL + CassandraUtils.getConnectionURI(props).toString();
        try {
            String host = props.getProperty(CassandraConstants.PROP_SERVER_NAME);
            int port = Integer.parseInt(props.getProperty(CassandraConstants.PROP_PORT_NUMBER));

            TSocket socket = new TSocket(host, port);
            transport = new TFramedTransport(socket);
            TProtocol protocol = new TBinaryProtocol(transport);
            client = new Cassandra.Client(protocol);
            socket.open();

            username = props.getProperty(CassandraConstants.PROP_USER);
            if (username != null) {
                String password = props.getProperty(CassandraConstants.PROP_PASSWORD);
                Map<String, String> credentials = new HashMap<String, String>();
                credentials.put("username", username);
                if (password != null) credentials.put("password", password);
                AuthenticationRequest authRequest = new AuthenticationRequest(credentials);
                client.login(authRequest);
            }

            client.set_keyspace(CassandraConstants.DEFAULT_KEYSPACE);
            String version = props.getProperty(CassandraConstants.PROP_CQL_VERSION);
            if (version != null) {
                try {
                    client.set_cql_version(version);
                    connectionProps.setProperty(CassandraConstants.PROP_ACTIVE_CQL_VERSION, version);
                } catch (Throwable e) {
                    // Ignore it
                }
            }
            {
                // Check features
                int majorVersion = getMetaData().getDatabaseMajorVersion();
                int minorVersion = getMetaData().getDatabaseMinorVersion();
                if (majorVersion == 1 && minorVersion >= 1 || majorVersion > 1) {
                    version11 = true;
                }
            }

            decoder = new ColumnDecoder(client.describe_keyspaces());

            currentKeyspace = props.getProperty(CassandraConstants.PROP_DATABASE_NAME, CassandraConstants.DEFAULT_KEYSPACE);
            structResultSet = Boolean.valueOf(props.getProperty(CassandraConstants.PROP_STRUCT_RESULT_SET));

            client.set_keyspace(currentKeyspace);
        } catch (InvalidRequestException e) {
            throw new SQLSyntaxErrorException(e);
        } catch (TException e) {
            throw new SQLNonTransientConnectionException(e);
        } catch (AuthenticationException e) {
            throw new SQLInvalidAuthorizationSpecException(e);
        } catch (AuthorizationException e) {
            throw new SQLInvalidAuthorizationSpecException(e);
        }
    }

    Cassandra.Client getClient()
    {
        return client;
    }

    String getCurrentKeyspace()
    {
        return currentKeyspace;
    }

    ColumnDecoder getDecoder()
    {
        return decoder;
    }

    String getUsername()
    {
        return username;
    }

    String getUrl()
    {
        return url;
    }

    public boolean isStructResultSet()
    {
        return structResultSet;
    }

    boolean isVersion11()
    {
        return version11;
    }

    private void checkNotClosed() throws SQLException
    {
        if (isClosed()) throw new SQLNonTransientConnectionException(ErrorMessages.WAS_CLOSED_CON);
    }

    public void clearWarnings() throws SQLException
    {
        // This implementation does not support the collection of warnings so clearing is a no-op
        // but it is still an exception to call this on a closed connection.
        checkNotClosed();
    }

    /**
     * On close of connection.
     */
    public synchronized void close() throws SQLException
    {
        // close all statements associated with this connection upon close
        for (Statement statement : statements)
            statement.close();
        statements.clear();

        if (isConnected()) {
            // then disconnect from the transport                
            disconnect();
        }
    }

    public void commit() throws SQLException
    {
        checkNotClosed();
        throw new SQLFeatureNotSupportedException("Cassandra connection is always in auto-commit mode");
    }

    public CassandraStatement createStatement() throws SQLException
    {
        checkNotClosed();
        CassandraStatement statement = new CassandraStatement(this);
        statements.add(statement);
        return statement;
    }

    public CassandraStatement createStatement(int resultSetType, int resultSetConcurrency) throws SQLException
    {
        checkNotClosed();
        CassandraStatement statement = new CassandraStatement(this, null, resultSetType, resultSetConcurrency);
        statements.add(statement);
        return statement;
    }

    public CassandraStatement createStatement(int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException
    {
        checkNotClosed();
        CassandraStatement statement = new CassandraStatement(this, null, resultSetType, resultSetConcurrency, resultSetHoldability);
        statements.add(statement);
        return statement;
    }

    public boolean getAutoCommit() throws SQLException
    {
        checkNotClosed();
        return true;
    }

    public Properties getConnectionProps()
    {
        return connectionProps;
    }

    public String getCatalog() throws SQLException
    {
        // This implementation does not support the catalog names so null is always returned if the connection is open.
        // but it is still an exception to call this on a closed connection.
        checkNotClosed();
        return null;
    }

    public Properties getClientInfo() throws SQLException
    {
        checkNotClosed();
        return clientInfo;
    }

    public String getClientInfo(String label) throws SQLException
    {
        checkNotClosed();
        return clientInfo.getProperty(label);
    }

    public int getHoldability() throws SQLException
    {
        checkNotClosed();
        // the rationale is there are really no commits in Cassandra so no boundary...
        return CassandraResultSet.DEFAULT_HOLDABILITY;
    }

    public DatabaseMetaData getMetaData() throws SQLException
    {
        checkNotClosed();
        synchronized (this) {
            if (meta == null) {
                meta = new CassandraDatabaseMetaData(this);
            }
        }
        return meta;
    }

    public int getTransactionIsolation() throws SQLException
    {
        checkNotClosed();
        return Connection.TRANSACTION_NONE;
    }

    public SQLWarning getWarnings() throws SQLException
    {
        checkNotClosed();
        // the rationale is there are no warnings to return in this implementation...
        return null;
    }

    public synchronized boolean isClosed() throws SQLException
    {

        return !isConnected();
    }

    public boolean isReadOnly() throws SQLException
    {
        checkNotClosed();
        return false;
    }

    public boolean isValid(int timeout) throws SQLException
    {
        checkNotClosed();
        if (timeout < 0) throw new SQLTimeoutException("Invalid timeout: " + timeout);

        // this needs to be more robust. Some query needs to be made to verify connection is really up.
        return !isClosed();
    }

    public boolean isWrapperFor(Class<?> arg0) throws SQLException
    {
        return false;
    }

    public String nativeSQL(String sql) throws SQLException
    {
        checkNotClosed();
        // the rationale is there are no distinction between grammars in this implementation...
        // so we are just return the input argument
        return sql;
    }

    public CassandraPreparedStatement prepareStatement(String sql) throws SQLException
    {
        checkNotClosed();
        CassandraPreparedStatement statement;
        if (version11) {
            statement = new CassandraPreparedStatementImpl(this, sql);
        } else {
            statement = new CassandraPreparedStatementLegacy(this, sql);
        }
        statements.add(statement);
        return statement;
    }

    @Override
    public CassandraPreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency) throws SQLException
    {
        return prepareStatement(sql);
    }

    @Override
    public CassandraPreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException
    {
        return prepareStatement(sql);
    }

    @Override
    public CassandraPreparedStatement prepareStatement(String sql, int autoGeneratedKeys) throws SQLException
    {
        return prepareStatement(sql);
    }

    @Override
    public CassandraPreparedStatement prepareStatement(String sql, int[] columnIndexes) throws SQLException
    {
        return prepareStatement(sql);
    }

    @Override
    public CassandraPreparedStatement prepareStatement(String sql, String[] columnNames) throws SQLException
    {
        return prepareStatement(sql);
    }

    public void rollback() throws SQLException
    {
        checkNotClosed();
        // just do nothing
    }

    public void setAutoCommit(boolean autoCommit) throws SQLException
    {
        checkNotClosed();
        // just do nothing
    }

    public void setCatalog(String arg0) throws SQLException
    {
        checkNotClosed();
        // the rationale is there are no catalog name to set in this implementation...
        // so we are "silently ignoring" the request
    }

    public void setClientInfo(Properties props) throws SQLClientInfoException
    {
        // we don't use them but we will happily collect them for now...
        if (props != null) clientInfo = props;
    }

    public void setClientInfo(String key, String value) throws SQLClientInfoException
    {
        // we don't use them but we will happily collect them for now...
        clientInfo.setProperty(key, value);
    }

    public void setHoldability(int arg0) throws SQLException
    {
        checkNotClosed();
        // the rationale is there are no holdability to set in this implementation...
        // so we are "silently ignoring" the request
    }

    public void setReadOnly(boolean arg0) throws SQLException
    {
        checkNotClosed();
        // the rationale is all connections are read/write in the Cassandra implementation...
        // so we are "silently ignoring" the request
    }

    public void setTransactionIsolation(int level) throws SQLException
    {
        checkNotClosed();
        if (level != Connection.TRANSACTION_NONE)
            throw new SQLFeatureNotSupportedException(ErrorMessages.NO_TRANSACTIONS);
    }

    public <T> T unwrap(Class<T> clazz) throws SQLException
    {
        throw new SQLFeatureNotSupportedException("Can't unwrap from " + clazz.getName());
    }

    /**
     * Execute a CQL query.
     *
     * @param queryStr    a CQL query string
     * @param maxRows     maximum rows. 0 or negative means unlimited
     * @param compression query compression to use
     * @return the query results encoded as a CqlResult structure
     * @throws InvalidRequestException     on poorly constructed or illegal requests
     * @throws UnavailableException        when not all required replicas could be created/read
     * @throws TimedOutException           when a cluster operation timed out
     * @throws SchemaDisagreementException when the client side and server side are at different versions of schema (Thrift)
     * @throws TException                  when there is a error in Thrift processing
     */
    protected CqlResult executeCQL(String queryStr, int maxRows, Compression compression)
        throws InvalidRequestException, UnavailableException, TimedOutException, SchemaDisagreementException, TException
    {
        currentKeyspace = CassandraUtils.determineCurrentKeyspace(queryStr, currentKeyspace);
        queryStr = CassandraUtils.modifyQueryLimits(queryStr, maxRows);
        return client.execute_cql_query(CassandraUtils.compressQuery(queryStr, compression), compression);
    }

    /**
     * Execute a CQL query using the default compression methodology.
     *
     * @param queryStr a CQL query string
     * @return the query results encoded as a CqlResult structure
     * @throws InvalidRequestException     on poorly constructed or illegal requests
     * @throws UnavailableException        when not all required replicas could be created/read
     * @throws TimedOutException           when a cluster operation timed out
     * @throws SchemaDisagreementException when the client side and server side are at different versions of schema (Thrift)
     * @throws TException                  when there is a error in Thrift processing
     */
    protected CqlResult executeCQL(String queryStr, int maxRows)
        throws InvalidRequestException, UnavailableException, TimedOutException, SchemaDisagreementException, TException
    {
        return executeCQL(queryStr, maxRows, defaultCompression);
    }

    /**
     * Remove a Statement from the Open Statements List
     */
    protected boolean removeStatement(Statement statement)
    {
        return statements.remove(statement);
    }

    /**
     * Shutdown the remote connection
     */
    protected void disconnect()
    {
        transport.close();
    }

    /**
     * Connection state.
     */
    protected boolean isConnected()
    {
        return transport.isOpen();
    }

    @Override
    public String getSchema()
    {
        return currentKeyspace;
    }

    @Override
    public void setSchema(String schema) throws SQLException
    {
        try {
            client.set_keyspace(schema);
        } catch (Exception e) {
            throw new SQLException("Can't change current keyspace to '" + schema + "'", e);
        }
        currentKeyspace = schema;
    }

}
