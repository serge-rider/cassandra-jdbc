package org.jkiss.jdbc.cassandra;

class LocalColumn {
    private String columnName;
    private int valueType;

    public LocalColumn(String columnName, int valueType)
    {
        this.columnName = columnName;
        this.valueType = valueType;
    }

    public String getColumnName()
    {
        return columnName;
    }

    public int getValueType()
    {
        return valueType;
    }
}
