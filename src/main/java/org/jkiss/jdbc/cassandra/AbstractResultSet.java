package org.jkiss.jdbc.cassandra;
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

import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.sql.*;
import java.util.Map;

abstract class AbstractResultSet implements ResultSet {

    public void cancelRowUpdates() throws SQLException
    {
        throw new SQLFeatureNotSupportedException();
    }

    public void deleteRow() throws SQLException
    {
        throw new SQLFeatureNotSupportedException();
    }

    public Array getArray(int index) throws SQLException
    {
        throw new SQLFeatureNotSupportedException();
    }

    public Array getArray(String name) throws SQLException
    {
        throw new SQLFeatureNotSupportedException();
    }

    public InputStream getAsciiStream(int index) throws SQLException
    {
        throw new SQLFeatureNotSupportedException();
    }

    public InputStream getAsciiStream(String name) throws SQLException
    {
        throw new SQLFeatureNotSupportedException();
    }

    public InputStream getBinaryStream(int index) throws SQLException
    {
        throw new SQLFeatureNotSupportedException();
    }

    public InputStream getBinaryStream(String name) throws SQLException
    {
        throw new SQLFeatureNotSupportedException();
    }

    public Blob getBlob(int index) throws SQLException
    {
        throw new SQLFeatureNotSupportedException();
    }

    public Blob getBlob(String name) throws SQLException
    {
        throw new SQLFeatureNotSupportedException();
    }

    public Reader getCharacterStream(int index) throws SQLException
    {
        throw new SQLFeatureNotSupportedException();
    }

    public Reader getCharacterStream(String name) throws SQLException
    {
        throw new SQLFeatureNotSupportedException();
    }

    public Clob getClob(int index) throws SQLException
    {
        throw new SQLFeatureNotSupportedException();
    }

    public Clob getClob(String name) throws SQLException
    {
        throw new SQLFeatureNotSupportedException();
    }

    public String getCursorName() throws SQLException
    {
        throw new SQLFeatureNotSupportedException();
    }

    public Reader getNCharacterStream(int index) throws SQLException
    {
        throw new SQLFeatureNotSupportedException();
    }

    public Reader getNCharacterStream(String name) throws SQLException
    {
        throw new SQLFeatureNotSupportedException();
    }

    public NClob getNClob(int index) throws SQLException
    {
        throw new SQLFeatureNotSupportedException();
    }

    public NClob getNClob(String name) throws SQLException
    {
        throw new SQLFeatureNotSupportedException();
    }

    public String getNString(int index) throws SQLException
    {
        throw new SQLFeatureNotSupportedException();
    }

    public String getNString(String name) throws SQLException
    {
        throw new SQLFeatureNotSupportedException();
    }

    public Object getObject(int arg0, Map<String, Class<?>> arg1) throws SQLException
    {
        throw new SQLFeatureNotSupportedException();
    }

    public Object getObject(String arg0, Map<String, Class<?>> arg1) throws SQLException
    {
        throw new SQLFeatureNotSupportedException();
    }

    public <T> T getObject(String columnLabel, Class<T> type) throws SQLException
    {
        throw new SQLFeatureNotSupportedException();
    }

    public <T> T getObject(int columnIndex, Class<T> type) throws SQLException
    {
        throw new SQLFeatureNotSupportedException();
    }

    public Ref getRef(int index) throws SQLException
    {
        throw new SQLFeatureNotSupportedException();
    }

    public Ref getRef(String name) throws SQLException
    {
        throw new SQLFeatureNotSupportedException();
    }


    public SQLXML getSQLXML(int index) throws SQLException
    {
        throw new SQLFeatureNotSupportedException();
    }

    public SQLXML getSQLXML(String name) throws SQLException
    {
        throw new SQLFeatureNotSupportedException();
    }

    public InputStream getUnicodeStream(int index) throws SQLException
    {
        throw new SQLFeatureNotSupportedException();
    }

    public InputStream getUnicodeStream(String name) throws SQLException
    {
        throw new SQLFeatureNotSupportedException();
    }

    public void insertRow() throws SQLException
    {
        throw new SQLFeatureNotSupportedException();
    }

    public void moveToCurrentRow() throws SQLException
    {
        throw new SQLFeatureNotSupportedException();
    }

    public void moveToInsertRow() throws SQLException
    {
        throw new SQLFeatureNotSupportedException();
    }

    public void refreshRow() throws SQLException
    {
        throw new SQLFeatureNotSupportedException();
    }

    public boolean rowDeleted() throws SQLException
    {
        throw new SQLFeatureNotSupportedException();
    }

    public boolean rowInserted() throws SQLException
    {
        throw new SQLFeatureNotSupportedException();
    }

    public boolean rowUpdated() throws SQLException
    {
        throw new SQLFeatureNotSupportedException();
    }

    //
    // all the update methods are unsupported, requires a separate statement in Cassandra
    //

    public void updateArray(int arg0, Array arg1) throws SQLException
    {
        throw new SQLFeatureNotSupportedException();
    }

    public void updateArray(String arg0, Array arg1) throws SQLException
    {
        throw new SQLFeatureNotSupportedException();
    }

    public void updateAsciiStream(int arg0, InputStream arg1) throws SQLException
    {
        throw new SQLFeatureNotSupportedException();
    }

    public void updateAsciiStream(int arg0, InputStream arg1, int arg2) throws SQLException
    {
        throw new SQLFeatureNotSupportedException();
    }

    public void updateAsciiStream(int arg0, InputStream arg1, long arg2) throws SQLException
    {
        throw new SQLFeatureNotSupportedException();
    }

    public void updateAsciiStream(String name, InputStream arg1) throws SQLException
    {
        throw new SQLFeatureNotSupportedException();
    }

    public void updateAsciiStream(String name, InputStream arg1, int arg2) throws SQLException
    {
        throw new SQLFeatureNotSupportedException();
    }

    public void updateAsciiStream(String name, InputStream arg1, long arg2) throws SQLException
    {
        throw new SQLFeatureNotSupportedException();
    }

    public void updateBigDecimal(int index, BigDecimal arg1) throws SQLException
    {
        throw new SQLFeatureNotSupportedException();
    }

    public void updateBigDecimal(String name, BigDecimal arg1) throws SQLException
    {
        throw new SQLFeatureNotSupportedException();
    }

    public void updateBinaryStream(int index, InputStream arg1) throws SQLException
    {
        throw new SQLFeatureNotSupportedException();
    }

    public void updateBinaryStream(int index, InputStream arg1, int arg2) throws SQLException
    {
        throw new SQLFeatureNotSupportedException();
    }

    public void updateBinaryStream(int index, InputStream arg1, long arg2) throws SQLException
    {
        throw new SQLFeatureNotSupportedException();
    }

    public void updateBinaryStream(String name, InputStream arg1) throws SQLException
    {
        throw new SQLFeatureNotSupportedException();
    }

    public void updateBinaryStream(String name, InputStream arg1, int arg2) throws SQLException
    {
        throw new SQLFeatureNotSupportedException();
    }

    public void updateBinaryStream(String name, InputStream arg1, long arg2) throws SQLException
    {
        throw new SQLFeatureNotSupportedException();
    }

    public void updateBlob(int index, Blob arg1) throws SQLException
    {
        throw new SQLFeatureNotSupportedException();
    }

    public void updateBlob(int index, InputStream arg1) throws SQLException
    {
        throw new SQLFeatureNotSupportedException();
    }

    public void updateBlob(int index, InputStream arg1, long arg2) throws SQLException
    {
        throw new SQLFeatureNotSupportedException();
    }

    public void updateBlob(String arg0, Blob arg1) throws SQLException
    {
        throw new SQLFeatureNotSupportedException();
    }

    public void updateBlob(String arg0, InputStream arg1) throws SQLException
    {
        throw new SQLFeatureNotSupportedException();
    }

    public void updateBlob(String arg0, InputStream arg1, long arg2) throws SQLException
    {
        throw new SQLFeatureNotSupportedException();
    }

    public void updateBoolean(int arg0, boolean arg1) throws SQLException
    {
        throw new SQLFeatureNotSupportedException();
    }

    public void updateBoolean(String arg0, boolean arg1) throws SQLException
    {
        throw new SQLFeatureNotSupportedException();
    }

    public void updateByte(int arg0, byte arg1) throws SQLException
    {
        throw new SQLFeatureNotSupportedException();
    }

    public void updateByte(String arg0, byte arg1) throws SQLException
    {
        throw new SQLFeatureNotSupportedException();
    }

    public void updateBytes(int arg0, byte[] arg1) throws SQLException
    {
        throw new SQLFeatureNotSupportedException();
    }

    public void updateBytes(String arg0, byte[] arg1) throws SQLException
    {
        throw new SQLFeatureNotSupportedException();
    }

    public void updateCharacterStream(int arg0, Reader arg1) throws SQLException
    {
        throw new SQLFeatureNotSupportedException();
    }

    public void updateCharacterStream(int arg0, Reader arg1, int arg2) throws SQLException
    {
        throw new SQLFeatureNotSupportedException();
    }

    public void updateCharacterStream(int arg0, Reader arg1, long arg2) throws SQLException
    {
        throw new SQLFeatureNotSupportedException();
    }

    public void updateCharacterStream(String arg0, Reader arg1) throws SQLException
    {
        throw new SQLFeatureNotSupportedException();
    }

    public void updateCharacterStream(String arg0, Reader arg1, int arg2) throws SQLException
    {
        throw new SQLFeatureNotSupportedException();
    }

    public void updateCharacterStream(String arg0, Reader arg1, long arg2) throws SQLException
    {
        throw new SQLFeatureNotSupportedException();
    }

    public void updateClob(int arg0, Clob arg1) throws SQLException
    {
        throw new SQLFeatureNotSupportedException();
    }

    public void updateClob(int arg0, Reader arg1) throws SQLException
    {
        throw new SQLFeatureNotSupportedException();
    }

    public void updateClob(int arg0, Reader arg1, long arg2) throws SQLException
    {
        throw new SQLFeatureNotSupportedException();
    }

    public void updateClob(String arg0, Clob arg1) throws SQLException
    {
        throw new SQLFeatureNotSupportedException();
    }

    public void updateClob(String arg0, Reader arg1) throws SQLException
    {
        throw new SQLFeatureNotSupportedException();
    }

    public void updateClob(String arg0, Reader arg1, long arg2) throws SQLException
    {
        throw new SQLFeatureNotSupportedException();
    }

    public void updateDate(int arg0, Date arg1) throws SQLException
    {
        throw new SQLFeatureNotSupportedException();
    }

    public void updateDate(String arg0, Date arg1) throws SQLException
    {
        throw new SQLFeatureNotSupportedException();
    }

    public void updateDouble(int arg0, double arg1) throws SQLException
    {
        throw new SQLFeatureNotSupportedException();
    }

    public void updateDouble(String arg0, double arg1) throws SQLException
    {
        throw new SQLFeatureNotSupportedException();
    }

    public void updateFloat(int arg0, float arg1) throws SQLException
    {
        throw new SQLFeatureNotSupportedException();
    }

    public void updateFloat(String arg0, float arg1) throws SQLException
    {
        throw new SQLFeatureNotSupportedException();
    }

    public void updateInt(int arg0, int arg1) throws SQLException
    {
        throw new SQLFeatureNotSupportedException();
    }

    public void updateInt(String arg0, int arg1) throws SQLException
    {
        throw new SQLFeatureNotSupportedException();
    }

    public void updateLong(int arg0, long arg1) throws SQLException
    {
        throw new SQLFeatureNotSupportedException();
    }

    public void updateLong(String arg0, long arg1) throws SQLException
    {
        throw new SQLFeatureNotSupportedException();
    }

    public void updateNCharacterStream(int arg0, Reader arg1) throws SQLException
    {
        throw new SQLFeatureNotSupportedException();
    }

    public void updateNCharacterStream(int arg0, Reader arg1, long arg2) throws SQLException
    {
        throw new SQLFeatureNotSupportedException();
    }

    public void updateNCharacterStream(String arg0, Reader arg1) throws SQLException
    {
        throw new SQLFeatureNotSupportedException();
    }

    public void updateNCharacterStream(String arg0, Reader arg1, long arg2) throws SQLException
    {
        throw new SQLFeatureNotSupportedException();
    }

    public void updateNClob(int arg0, NClob arg1) throws SQLException
    {
        throw new SQLFeatureNotSupportedException();
    }

    public void updateNClob(int arg0, Reader arg1) throws SQLException
    {
        throw new SQLFeatureNotSupportedException();
    }

    public void updateNClob(int arg0, Reader arg1, long arg2) throws SQLException
    {
        throw new SQLFeatureNotSupportedException();
    }

    public void updateNClob(String arg0, NClob arg1) throws SQLException
    {
        throw new SQLFeatureNotSupportedException();
    }

    public void updateNClob(String arg0, Reader arg1) throws SQLException
    {
        throw new SQLFeatureNotSupportedException();
    }

    public void updateNClob(String arg0, Reader arg1, long arg2) throws SQLException
    {
        throw new SQLFeatureNotSupportedException();
    }

    public void updateNString(int arg0, String arg1) throws SQLException
    {
        throw new SQLFeatureNotSupportedException();
    }

    public void updateNString(String arg0, String arg1) throws SQLException
    {
        throw new SQLFeatureNotSupportedException();
    }

    public void updateNull(int index) throws SQLException
    {
        throw new SQLFeatureNotSupportedException();
    }

    public void updateNull(String name) throws SQLException
    {
        throw new SQLFeatureNotSupportedException();
    }

    public void updateObject(int arg0, Object arg1) throws SQLException
    {
        throw new SQLFeatureNotSupportedException();
    }

    public void updateObject(int arg0, Object arg1, int arg2) throws SQLException
    {
        throw new SQLFeatureNotSupportedException();
    }

    public void updateObject(String arg0, Object arg1) throws SQLException
    {
        throw new SQLFeatureNotSupportedException();
    }

    public void updateObject(String arg0, Object arg1, int arg2) throws SQLException
    {
        throw new SQLFeatureNotSupportedException();
    }

    public void updateRef(int arg0, Ref arg1) throws SQLException
    {
        throw new SQLFeatureNotSupportedException();
    }

    public void updateRef(String arg0, Ref arg1) throws SQLException
    {
        throw new SQLFeatureNotSupportedException();
    }

    public void updateRow() throws SQLException
    {
        throw new SQLFeatureNotSupportedException();
    }

    public void updateRowId(int arg0, RowId arg1) throws SQLException
    {
        throw new SQLFeatureNotSupportedException();
    }

    public void updateRowId(String arg0, RowId arg1) throws SQLException
    {
        throw new SQLFeatureNotSupportedException();
    }

    public void updateShort(int arg0, short arg1) throws SQLException
    {
        throw new SQLFeatureNotSupportedException();
    }

    public void updateShort(String arg0, short arg1) throws SQLException
    {
        throw new SQLFeatureNotSupportedException();
    }

    public void updateSQLXML(int arg0, SQLXML arg1) throws SQLException
    {
        throw new SQLFeatureNotSupportedException();
    }

    public void updateSQLXML(String arg0, SQLXML arg1) throws SQLException
    {
        throw new SQLFeatureNotSupportedException();
    }

    public void updateString(int arg0, String arg1) throws SQLException
    {
        throw new SQLFeatureNotSupportedException();
    }

    public void updateString(String arg0, String arg1) throws SQLException
    {
        throw new SQLFeatureNotSupportedException();
    }

    public void updateTime(int arg0, Time arg1) throws SQLException
    {
        throw new SQLFeatureNotSupportedException();
    }

    public void updateTime(String arg0, Time arg1) throws SQLException
    {
        throw new SQLFeatureNotSupportedException();
    }

    public void updateTimestamp(int arg0, Timestamp arg1) throws SQLException
    {
        throw new SQLFeatureNotSupportedException();
    }

    public void updateTimestamp(String arg0, Timestamp arg1) throws SQLException
    {
        throw new SQLFeatureNotSupportedException();
    }
}
