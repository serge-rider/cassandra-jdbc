package org.jkiss.jdbc.cassandra;

import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.Struct;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * JDBC struct
 */
public class CassandraStruct implements Struct, ResultSetMetaData {

    private final CassandraResultSet resultSet;
    private List<TypedColumn> columns = new ArrayList<TypedColumn>();

    public CassandraStruct(CassandraResultSet resultSet)
    {
        this.resultSet = resultSet;
    }

    public ResultSetMetaData getMetaData()
    {
        return this;
    }

    @Override
    public String getSQLTypeName() throws SQLException
    {
        return CassandraConstants.ROW_TYPE_NAME;
    }

    @Override
    public Object[] getAttributes() throws SQLException
    {
        Object[] values = new Object[columns.size()];
        for (int i = 0, columnsSize = columns.size(); i < columnsSize; i++) {
            TypedColumn column = columns.get(i);
            values[i] = column.getValue();
        }
        return values;
    }

    @Override
    public Object[] getAttributes(Map<String, Class<?>> map) throws SQLException
    {
        return getAttributes();
    }

    public List<TypedColumn> getColumns()
    {
        return columns;
    }

    void addColumn(TypedColumn column)
    {
        columns.add(column);
    }

    public String stringRepresentation()
    {
        StringBuilder str = new StringBuilder(100);
        for (TypedColumn column : columns) {
            if (str.length() > 0) str.append(',');
            str.append(column.getNameString()).append(':').append(column.getValueString());
        }
        return str.toString();
    }

    @Override
    public String toString()
    {
        return stringRepresentation();
    }

    ///////////////////////////////////////////////////////////////////
    // ResultSetMetaData
    ///////////////////////////////////////////////////////////////////

    @Override
    public int getColumnCount() throws SQLException
    {
        return columns.size();
    }

    @Override
    public boolean isAutoIncrement(int column) throws SQLException
    {
        return false;
    }

    @Override
    public boolean isCaseSensitive(int column) throws SQLException
    {
        return true;
    }

    @Override
    public boolean isSearchable(int column) throws SQLException
    {
        return false;
    }

    @Override
    public boolean isCurrency(int column) throws SQLException
    {
        return false;
    }

    @Override
    public int isNullable(int column) throws SQLException
    {
        return columnNullable;
    }

    @Override
    public boolean isSigned(int column) throws SQLException
    {
        return columns.get(column - 1).getValueType().isSigned();
    }

    @Override
    public int getColumnDisplaySize(int column) throws SQLException
    {
        TypedColumn col = columns.get(column - 1);
        return col.getValueType().getPrecision(col.getValue());
    }

    @Override
    public String getColumnLabel(int column) throws SQLException
    {
        return columns.get(column - 1).getNameString();
    }

    @Override
    public String getColumnName(int column) throws SQLException
    {
        return columns.get(column - 1).getNameString();
    }

    @Override
    public String getSchemaName(int column) throws SQLException
    {
        return resultSet.getKeyspace();
    }

    @Override
    public int getPrecision(int column) throws SQLException
    {
        TypedColumn col = columns.get(column - 1);
        return col.getValueType().getPrecision(col.getValue());
    }

    @Override
    public int getScale(int column) throws SQLException
    {
        TypedColumn col = columns.get(column - 1);
        return col.getValueType().getScale(col.getValue());
    }

    @Override
    public String getTableName(int column) throws SQLException
    {
        return resultSet.getColumnFamily();
    }

    @Override
    public String getCatalogName(int column) throws SQLException
    {
        return resultSet.getConnection().getCatalog();
    }

    @Override
    public int getColumnType(int column) throws SQLException
    {
        return columns.get(column - 1).getValueType().getJdbcType();
    }

    @Override
    public String getColumnTypeName(int column) throws SQLException
    {
        TypedColumn col = columns.get(column - 1);
        return col.getValueType().getType().getSimpleName();
    }

    @Override
    public boolean isReadOnly(int column) throws SQLException
    {
        return false;
    }

    @Override
    public boolean isWritable(int column) throws SQLException
    {
        return true;
    }

    @Override
    public boolean isDefinitelyWritable(int column) throws SQLException
    {
        return true;
    }

    @Override
    public String getColumnClassName(int column) throws SQLException
    {
        TypedColumn col = columns.get(column - 1);
        return col.getValueType().getType().getName();
    }

    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException
    {
        throw new SQLFeatureNotSupportedException("Can't unwrap " + iface.getName());
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException
    {
        return false;
    }
}
