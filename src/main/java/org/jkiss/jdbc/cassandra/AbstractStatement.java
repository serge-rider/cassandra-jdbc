package org.jkiss.jdbc.cassandra;

import java.io.InputStream;
import java.io.Reader;
import java.sql.*;

abstract class AbstractStatement implements Statement {
    /*
     * From the Statement Implementation
     */
    public void cancel() throws SQLException
    {
        throw new SQLFeatureNotSupportedException();
    }

    public ResultSet getGeneratedKeys() throws SQLException
    {
        throw new SQLFeatureNotSupportedException();
    }

    public void setCursorName(String arg0) throws SQLException
    {
        throw new SQLFeatureNotSupportedException();
    }

    public void closeOnCompletion() throws SQLException
    {
        throw new SQLFeatureNotSupportedException();
    }

    public boolean isCloseOnCompletion() throws SQLException
    {
        throw new SQLFeatureNotSupportedException();
    }

    /*
    * From the PreparedStatement Implementation
    */

    public void setArray(int parameterIndex, Array x) throws SQLException
    {
        throw new SQLFeatureNotSupportedException();
    }

    public void setAsciiStream(int parameterIndex, InputStream x) throws SQLException
    {
        throw new SQLFeatureNotSupportedException();
    }

    public void setAsciiStream(int parameterIndex, InputStream x, int length) throws SQLException
    {
        throw new SQLFeatureNotSupportedException();
    }

    public void setAsciiStream(int parameterIndex, InputStream x, long length) throws SQLException
    {
        throw new SQLFeatureNotSupportedException();
    }

    public void setBinaryStream(int parameterIndex, InputStream x) throws SQLException
    {
        throw new SQLFeatureNotSupportedException();
    }

    public void setBinaryStream(int parameterIndex, InputStream x, int length) throws SQLException
    {
        throw new SQLFeatureNotSupportedException();
    }

    public void setBinaryStream(int parameterIndex, InputStream x, long length) throws SQLException
    {
        throw new SQLFeatureNotSupportedException();
    }

    public void setBlob(int parameterIndex, Blob x) throws SQLException
    {
        throw new SQLFeatureNotSupportedException();
    }

    public void setBlob(int parameterIndex, InputStream inputStream) throws SQLException
    {
        throw new SQLFeatureNotSupportedException();
    }

    public void setBlob(int parameterIndex, InputStream inputStream, long length) throws SQLException
    {
        throw new SQLFeatureNotSupportedException();
    }

    public void setCharacterStream(int parameterIndex, Reader reader) throws SQLException
    {
        throw new SQLFeatureNotSupportedException();
    }

    public void setCharacterStream(int parameterIndex, Reader reader, int length) throws SQLException
    {
        throw new SQLFeatureNotSupportedException();
    }

    public void setCharacterStream(int parameterIndex, Reader reader, long length) throws SQLException
    {
        throw new SQLFeatureNotSupportedException();
    }

    public void setClob(int parameterIndex, Clob x) throws SQLException
    {
        throw new SQLFeatureNotSupportedException("method not supported");
    }

    public void setClob(int parameterIndex, Reader reader) throws SQLException
    {
        throw new SQLFeatureNotSupportedException();
    }

    public void setClob(int parameterIndex, Reader reader, long length) throws SQLException
    {
        throw new SQLFeatureNotSupportedException();
    }

    public void setNCharacterStream(int parameterIndex, Reader value) throws SQLException
    {
        throw new SQLFeatureNotSupportedException();
    }

    public void setNCharacterStream(int parameterIndex, Reader value, long length) throws SQLException
    {
        throw new SQLFeatureNotSupportedException();
    }

    public void setNClob(int parameterIndex, NClob value) throws SQLException
    {
        throw new SQLFeatureNotSupportedException();
    }

    public void setNClob(int parameterIndex, Reader reader) throws SQLException
    {
        throw new SQLFeatureNotSupportedException();
    }

    public void setNClob(int parameterIndex, Reader reader, long length) throws SQLException
    {
        throw new SQLFeatureNotSupportedException();
    }

//    public void setObject(int parameterIndex, Object x, int targetSqlType) throws SQLException
//    {
//        throw new SQLFeatureNotSupportedException();
//    }


//    public void setObject(int parameterIndex, Object x, int targetSqlType, int scaleOrLength) throws SQLException
//    {
//        throw new SQLFeatureNotSupportedException();
//    }


    public void setRef(int parameterIndex, Ref x) throws SQLException
    {
        throw new SQLFeatureNotSupportedException();
    }

    public void setSQLXML(int parameterIndex, SQLXML xmlObject) throws SQLException
    {
        throw new SQLFeatureNotSupportedException();
    }

    public void setUnicodeStream(int parameterIndex, InputStream x, int length) throws SQLException
    {
        throw new SQLFeatureNotSupportedException();
    }

}
