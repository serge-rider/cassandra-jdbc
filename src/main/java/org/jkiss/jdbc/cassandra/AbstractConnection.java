package org.jkiss.jdbc.cassandra;

import java.sql.*;
import java.util.Map;
import java.util.concurrent.Executor;

/**
 * Abstract connection
 */
public abstract class AbstractConnection implements Connection {


    /////////////////////////////////////////////////////
    // Factories

    public Array createArrayOf(String arg0, Object[] arg1) throws SQLException
    {
        throw new SQLFeatureNotSupportedException();
    }

    public Blob createBlob() throws SQLException
    {
        throw new SQLFeatureNotSupportedException();
    }

    public Clob createClob() throws SQLException
    {
        throw new SQLFeatureNotSupportedException();
    }

    public NClob createNClob() throws SQLException
    {
        throw new SQLFeatureNotSupportedException();
    }

    public SQLXML createSQLXML() throws SQLException
    {
        throw new SQLFeatureNotSupportedException();
    }

    public Struct createStruct(String arg0, Object[] arg1) throws SQLException
    {
        throw new SQLFeatureNotSupportedException();
    }

    public Map<String, Class<?>> getTypeMap() throws SQLException
    {
        throw new SQLFeatureNotSupportedException();
    }

    public CallableStatement prepareCall(String arg0) throws SQLException
    {
        throw new SQLFeatureNotSupportedException();
    }

    public CallableStatement prepareCall(String arg0, int arg1, int arg2) throws SQLException
    {
        throw new SQLFeatureNotSupportedException();
    }

    public CallableStatement prepareCall(String arg0, int arg1, int arg2, int arg3) throws SQLException
    {
        throw new SQLFeatureNotSupportedException();
    }

    public void releaseSavepoint(Savepoint arg0) throws SQLException
    {
        throw new SQLFeatureNotSupportedException();
    }

    public void rollback(Savepoint arg0) throws SQLException
    {
        throw new SQLFeatureNotSupportedException();
    }

    public Savepoint setSavepoint() throws SQLException
    {
        throw new SQLFeatureNotSupportedException();
    }

    public Savepoint setSavepoint(String arg0) throws SQLException
    {
        throw new SQLFeatureNotSupportedException();
    }

    public void setTypeMap(Map<String, Class<?>> arg0) throws SQLException
    {
        throw new SQLFeatureNotSupportedException();
    }

    public void abort(Executor executor) throws SQLException
    {
        throw new SQLFeatureNotSupportedException();
    }

    public void setNetworkTimeout(Executor executor, int milliseconds) throws SQLException
    {
        throw new SQLFeatureNotSupportedException();
    }

    public int getNetworkTimeout() throws SQLException
    {
        throw new SQLFeatureNotSupportedException();
    }

}
