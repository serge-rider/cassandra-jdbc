package org.jkiss.jdbc.cassandra;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.URL;
import java.nio.ByteBuffer;
import java.sql.*;
import java.util.Calendar;

/**
 * Local result set
 */
class LocalResultSet extends AbstractResultSet implements ResultSet {
    private Statement statement;
    private int rowNumber = 0;
    private int fetchDirection = FETCH_FORWARD;

    /**
     * The values.
     */
    private LocalColumn[] columns;
    private Object[][] rows;
    private boolean wasNull;

    LocalResultSet()
    {
        this(null, new LocalColumn[0], new Object[0][0]);
    }

    LocalResultSet(Statement statement, LocalColumn[] columns, Object[][] rows)
    {
        this.statement = statement;
        this.columns = columns;
        this.rows = rows;
    }

    private boolean hasMoreRows()
    {
        return rowNumber < rows.length;
    }

    public boolean absolute(int rowNum) throws SQLException
    {
        rowNumber = rowNum;
        return rowNumber > 0 && rowNumber <= rows.length;
    }

    public void afterLast() throws SQLException
    {
        rowNumber = rows.length + 1;
    }

    public void beforeFirst() throws SQLException
    {
        rowNumber = 0;
    }

    private void checkIndex(int index) throws SQLException
    {
        // 1 <= index <= size()
        if (index < 1 || index > rows.length)
            throw new SQLSyntaxErrorException("Bad row number: " + index);
    }

    public void clearWarnings() throws SQLException
    {
    }

    public void close() throws SQLException
    {
        columns = new LocalColumn[0];
        rows = new Object[0][0];
    }

    public int findColumn(String name) throws SQLException
    {
        for (int i = 0; i < columns.length; i++) {
            if (columns[i].getColumnName().equalsIgnoreCase(name)) {
                return i + 1;
            }
        }
        throw new SQLSyntaxErrorException("Column '" + name + "' not found");
    }

    public boolean first() throws SQLException
    {
        rowNumber = 1;
        return rowNumber <= rows.length;
    }

    public BigDecimal getBigDecimal(int index) throws SQLException
    {
        checkIndex(index);
        return getBigDecimal(rows[rowNumber - 1][index - 1]);
    }

    /**
     * @deprecated
     */
    public BigDecimal getBigDecimal(int index, int scale) throws SQLException
    {
        checkIndex(index);
        return getBigDecimal(rows[rowNumber - 1][index - 1]).setScale(scale);
    }

    public BigDecimal getBigDecimal(String name) throws SQLException
    {
        return getBigDecimal(rows[rowNumber - 1][findColumn(name) - 1]);
    }

    /**
     * @deprecated
     */
    public BigDecimal getBigDecimal(String name, int scale) throws SQLException
    {
        return getBigDecimal(rows[rowNumber - 1][findColumn(name) - 1]).setScale(scale);
    }

    private BigDecimal getBigDecimal(Object value) throws SQLException
    {
        wasNull = value == null;

        if (wasNull) return null;

        if (value instanceof BigDecimal) return (BigDecimal) value;

        if (value instanceof Long) return BigDecimal.valueOf((Long) value);

        if (value instanceof Double) return BigDecimal.valueOf((Double) value);

        if (value instanceof BigInteger) return new BigDecimal((BigInteger) value);

        try {
            if (value instanceof String) return (new BigDecimal((String) value));
        } catch (NumberFormatException e) {
            throw new SQLSyntaxErrorException(e);
        }

        throw new SQLSyntaxErrorException(String.format(ErrorMessages.NOT_TRANSLATABLE, value.getClass().getSimpleName(), "BigDecimal"));
    }

    public BigInteger getBigInteger(int index) throws SQLException
    {
        checkIndex(index);
        return getBigInteger(rows[rowNumber - 1][index - 1]);
    }

    public BigInteger getBigInteger(String name) throws SQLException
    {
        return getBigInteger(rows[rowNumber - 1][findColumn(name) - 1]);
    }

    private BigInteger getBigInteger(Object value) throws SQLException
    {
        wasNull = value == null;

        if (wasNull) return null;

        if (value instanceof BigInteger) return (BigInteger) value;

        if (value instanceof Integer) return BigInteger.valueOf((Integer) value);

        if (value instanceof Long) return BigInteger.valueOf((Long) value);

        try {
            if (value instanceof String) return (new BigInteger((String) value));
        } catch (NumberFormatException e) {
            throw new SQLSyntaxErrorException(e);
        }

        throw new SQLSyntaxErrorException(String.format(ErrorMessages.NOT_TRANSLATABLE, value.getClass().getSimpleName(), "BigInteger"));
    }

    public boolean getBoolean(int index) throws SQLException
    {
        checkIndex(index);
        return getBoolean(rows[rowNumber - 1][index - 1]);
    }

    public boolean getBoolean(String name) throws SQLException
    {
        return getBoolean(rows[rowNumber - 1][findColumn(name) - 1]);
    }

    private Boolean getBoolean(Object value) throws SQLException
    {
        wasNull = value == null;

        if (wasNull) return false;

        if (value instanceof Boolean) return (Boolean) value;

        if (value instanceof Integer) return ((Integer) value) == 0;

        if (value instanceof Long) return (Long) value == 0;

        if (value instanceof BigInteger) return ((BigInteger) value).intValue() == 0;

        if (value instanceof String) {
            return Boolean.valueOf(value.toString());
        }

        throw new SQLSyntaxErrorException(String.format(ErrorMessages.NOT_TRANSLATABLE, value.getClass().getSimpleName(), "Boolean"));
    }

    public byte getByte(int index) throws SQLException
    {
        checkIndex(index);
        return getByte(rows[rowNumber - 1][index - 1]);
    }

    public byte getByte(String name) throws SQLException
    {
        return getByte(rows[rowNumber - 1][findColumn(name) - 1]);
    }

    private Byte getByte(Object value) throws SQLException
    {
        wasNull = value == null;

        if (wasNull) return 0;

        if (value instanceof Integer) return ((Integer) value).byteValue();

        if (value instanceof Long) return ((Long) value).byteValue();

        if (value instanceof BigInteger) return ((BigInteger) value).byteValue();

        try {
            if (value instanceof String) return (new Byte((String) value));
        } catch (NumberFormatException e) {
            throw new SQLSyntaxErrorException(e);
        }

        throw new SQLSyntaxErrorException(String.format(ErrorMessages.NOT_TRANSLATABLE, value.getClass().getSimpleName(), "Byte"));
    }

    public byte[] getBytes(int index) throws SQLException
    {
        return getBytes(rows[rowNumber - 1][index - 1]);
    }

    public byte[] getBytes(String name) throws SQLException
    {
        return getBytes(rows[rowNumber - 1][findColumn(name) - 1]);
    }

    private byte[] getBytes(Object value) throws SQLException
    {
        wasNull = value == null;
        return value instanceof ByteBuffer ? ((ByteBuffer) value).array() : null;
    }

    public int getConcurrency() throws SQLException
    {
        return statement.getResultSetConcurrency();
    }

    public Date getDate(int index) throws SQLException
    {
        checkIndex(index);
        return getDate(rows[rowNumber - 1][index - 1]);
    }

    public Date getDate(int index, Calendar calendar) throws SQLException
    {
        checkIndex(index);
        // silently ignore the Calendar argument; its a hint we do not need
        return getDate(index);
    }

    public Date getDate(String name) throws SQLException
    {
        return getDate(rows[rowNumber - 1][findColumn(name) - 1]);
    }

    public Date getDate(String name, Calendar calendar) throws SQLException
    {
        // silently ignore the Calendar argument; its a hint we do not need
        return getDate(name);
    }

    private Date getDate(Object value) throws SQLException
    {
        wasNull = value == null;

        if (wasNull) return null;

        if (value instanceof Long) return new Date((Long) value);

        if (value instanceof java.util.Date) return new Date(((java.util.Date) value).getTime());

        try {
            if (value instanceof String) return Date.valueOf((String) value);
        } catch (IllegalArgumentException e) {
            throw new SQLSyntaxErrorException(e);
        }

        throw new SQLSyntaxErrorException(String.format(ErrorMessages.NOT_TRANSLATABLE, value.getClass().getSimpleName(), "SQL Date"));
    }

    public double getDouble(int index) throws SQLException
    {
        checkIndex(index);
        return getDouble(rows[rowNumber - 1][index - 1]);
    }

    public double getDouble(String name) throws SQLException
    {
        return getDouble(rows[rowNumber - 1][findColumn(name) - 1]);
    }

    private Double getDouble(Object value) throws SQLException
    {
        wasNull = value == null;

        if (wasNull) return 0.0;

        if (value instanceof Double) return ((Double) value);

        if (value instanceof Float) return ((Float) value).doubleValue();

        if (value instanceof Integer) return Double.valueOf((Integer) value);

        if (value instanceof Long) return Double.valueOf((Long) value);

        if (value instanceof BigInteger) return ((BigInteger) value).doubleValue();

        try {
            if (value instanceof String) return new Double((String) value);
        } catch (NumberFormatException e) {
            throw new SQLSyntaxErrorException(e);
        }

        throw new SQLSyntaxErrorException(String.format(ErrorMessages.NOT_TRANSLATABLE, value.getClass().getSimpleName(), "Double"));
    }

    public int getFetchDirection() throws SQLException
    {
        return fetchDirection;
    }

    public int getFetchSize() throws SQLException
    {
        return 1;
    }

    public float getFloat(int index) throws SQLException
    {
        checkIndex(index);
        return getFloat(rows[rowNumber - 1][index - 1]);
    }

    public float getFloat(String name) throws SQLException
    {
        return getFloat(rows[rowNumber - 1][findColumn(name) - 1]);
    }

    private Float getFloat(Object value) throws SQLException
    {
        wasNull = value == null;

        if (wasNull) return (float) 0.0;

        if (value instanceof Float) return ((Float) value);

        if (value instanceof Double) return ((Double) value).floatValue();

        if (value instanceof Integer) return Float.valueOf((Integer) value);

        if (value instanceof Long) return Float.valueOf((Long) value);

        if (value instanceof BigInteger) return ((BigInteger) value).floatValue();

        try {
            if (value instanceof String) return new Float((String) value);
        } catch (NumberFormatException e) {
            throw new SQLException(e);
        }

        throw new SQLSyntaxErrorException(String.format(ErrorMessages.NOT_TRANSLATABLE, value.getClass().getSimpleName(), "Float"));
    }

    public int getHoldability() throws SQLException
    {
        return statement.getResultSetHoldability();
    }

    public int getInt(int index) throws SQLException
    {
        checkIndex(index);
        return getInt(rows[rowNumber - 1][index - 1]);
    }

    public int getInt(String name) throws SQLException
    {
        return getInt(rows[rowNumber - 1][findColumn(name) - 1]);
    }

    private int getInt(Object value) throws SQLException
    {
        wasNull = value == null;

        if (wasNull) return 0;

        if (value instanceof Integer) return ((Integer) value);

        if (value instanceof Long) return ((Long) value).intValue();

        if (value instanceof BigInteger) return ((BigInteger) value).intValue();

        try {
            if (value instanceof String) return (Integer.parseInt((String) value));
        } catch (NumberFormatException e) {
            throw new SQLSyntaxErrorException(e);
        }

        throw new SQLSyntaxErrorException(String.format(ErrorMessages.NOT_TRANSLATABLE, value.getClass().getSimpleName(), "int"));
    }

    public long getLong(int index) throws SQLException
    {
        checkIndex(index);
        return getLong(rows[rowNumber - 1][index - 1]);
    }

    public long getLong(String name) throws SQLException
    {
        return getLong(rows[rowNumber - 1][findColumn(name) - 1]);
    }

    private Long getLong(Object value) throws SQLException
    {
        wasNull = value == null;

        if (wasNull) return 0L;

        if (value instanceof Long) return (Long) value;

        if (value instanceof Integer) return Long.valueOf((Integer) value);

        if (value instanceof BigInteger) return getBigInteger(value).longValue();

        if (value instanceof Long) return (Long) value;

        try {
            if (value instanceof String) return (Long.parseLong((String) value));
        } catch (NumberFormatException e) {
            throw new SQLSyntaxErrorException(e);
        }

        throw new SQLSyntaxErrorException(String.format(ErrorMessages.NOT_TRANSLATABLE, value.getClass().getSimpleName(), "Long"));
    }

    public ResultSetMetaData getMetaData() throws SQLException
    {
        return new MetaData();
    }

    public Object getObject(int index) throws SQLException
    {
        checkIndex(index);
        return getObject(rows[rowNumber - 1][index - 1]);
    }

    public Object getObject(String name) throws SQLException
    {
        return getObject(rows[rowNumber - 1][findColumn(name) - 1]);
    }


    private Object getObject(Object value) throws SQLException
    {
        wasNull = value == null;
        return value;
    }

    public int getRow() throws SQLException
    {
        return rowNumber;
    }

    public RowId getRowId(int index) throws SQLException
    {
        checkIndex(index);
        return getRowId(rows[rowNumber - 1][index - 1]);
    }

    public RowId getRowId(String name) throws SQLException
    {
        return getRowId(rows[rowNumber - 1][findColumn(name) - 1]);
    }

    private RowId getRowId(Object value) throws SQLException
    {
        wasNull = value == null;
        return value instanceof ByteBuffer ? new CassandraRowId((ByteBuffer) value) : null;
    }

    public short getShort(int index) throws SQLException
    {
        checkIndex(index);
        return getShort(rows[rowNumber - 1][index - 1]);
    }

    public short getShort(String name) throws SQLException
    {
        return getShort(rows[rowNumber - 1][findColumn(name) - 1]);
    }

    private Short getShort(Object value) throws SQLException
    {
        wasNull = value == null;

        if (wasNull) return 0;

        if (value instanceof Integer) return ((Integer) value).shortValue();

        if (value instanceof Long) return ((Long) value).shortValue();

        if (value instanceof BigInteger) return ((BigInteger) value).shortValue();

        try {
            if (value instanceof String) return (new Short((String) value));
        } catch (NumberFormatException e) {
            throw new SQLSyntaxErrorException(e);
        }

        throw new SQLSyntaxErrorException(String.format(ErrorMessages.NOT_TRANSLATABLE, value.getClass().getSimpleName(), "Short"));
    }

    public Statement getStatement() throws SQLException
    {
        return statement;
    }

    public String getString(int index) throws SQLException
    {
        checkIndex(index);
        return getString(rows[rowNumber - 1][index - 1]);
    }

    public String getString(String name) throws SQLException
    {
        return getString(rows[rowNumber - 1][findColumn(name) - 1]);
    }

    private String getString(Object value) throws SQLException
    {
        wasNull = value == null;
        return (wasNull) ? null : value.toString();
    }

    public Time getTime(int index) throws SQLException
    {
        return getTime(rows[rowNumber - 1][index - 1]);
    }

    public Time getTime(int index, Calendar calendar) throws SQLException
    {
        checkIndex(index);
        // silently ignore the Calendar argument; its a hint we do not need
        return getTime(index);
    }

    public Time getTime(String name) throws SQLException
    {
        return getTime(rows[rowNumber - 1][findColumn(name) - 1]);
    }

    public Time getTime(String name, Calendar calendar) throws SQLException
    {
        // silently ignore the Calendar argument; its a hint we do not need
        return getTime(rows[rowNumber - 1][findColumn(name) - 1]);
    }

    private Time getTime(Object value) throws SQLException
    {
        wasNull = value == null;

        if (wasNull) return null;

        if (value instanceof Long) return new Time((Long) value);

        if (value instanceof java.util.Date) return new Time(((java.util.Date) value).getTime());

        try {
            if (value instanceof String) return Time.valueOf((String) value);
        } catch (IllegalArgumentException e) {
            throw new SQLSyntaxErrorException(e);
        }

        throw new SQLSyntaxErrorException(String.format(ErrorMessages.NOT_TRANSLATABLE, value.getClass().getSimpleName(), "SQL Time"));
    }

    public Timestamp getTimestamp(int index) throws SQLException
    {
        checkIndex(index);
        return getTimestamp(rows[rowNumber - 1][index - 1]);
    }

    public Timestamp getTimestamp(int index, Calendar calendar) throws SQLException
    {
        checkIndex(index);
        // silently ignore the Calendar argument; its a hint we do not need
        return getTimestamp(rows[rowNumber - 1][index - 1]);
    }

    public Timestamp getTimestamp(String name) throws SQLException
    {
        return getTimestamp(rows[rowNumber - 1][findColumn(name) - 1]);
    }

    public Timestamp getTimestamp(String name, Calendar calendar) throws SQLException
    {
        // silently ignore the Calendar argument; its a hint we do not need
        return getTimestamp(rows[rowNumber - 1][findColumn(name) - 1]);
    }

    private Timestamp getTimestamp(Object value) throws SQLException
    {
        wasNull = value == null;

        if (wasNull) return null;

        if (value instanceof Long) return new Timestamp((Long) value);

        if (value instanceof java.util.Date) return new Timestamp(((java.util.Date) value).getTime());

        try {
            if (value instanceof String) return Timestamp.valueOf((String) value);
        } catch (IllegalArgumentException e) {
            throw new SQLSyntaxErrorException(e);
        }

        throw new SQLSyntaxErrorException(String.format(ErrorMessages.NOT_TRANSLATABLE, value.getClass().getSimpleName(), "SQL Timestamp"));
    }

    public int getType() throws SQLException
    {
        return TYPE_FORWARD_ONLY;
    }

    // URL (awaiting some clarifications as to how it is stored in C* ... just a validated Sting in URL format?
    public URL getURL(int arg0) throws SQLException
    {
        throw new SQLFeatureNotSupportedException();
    }

    public URL getURL(String arg0) throws SQLException
    {
        throw new SQLFeatureNotSupportedException();
    }

    public SQLWarning getWarnings() throws SQLException
    {
        return null;
    }


    public boolean isAfterLast() throws SQLException
    {
        return rowNumber > rows.length;
    }

    public boolean isBeforeFirst() throws SQLException
    {
        return rowNumber == 0;
    }

    public boolean isClosed() throws SQLException
    {
        return columns.length == 0;
    }

    public boolean isFirst() throws SQLException
    {
        return rowNumber == 1;
    }

    public boolean isLast() throws SQLException
    {
        return rowNumber > 0 && rowNumber == rows.length;
    }

    public boolean isWrapperFor(Class<?> clazz) throws SQLException
    {
        return false;
    }

    public boolean last() throws SQLException
    {
        rowNumber = rows.length;
        return rowNumber > 0;
    }

    public synchronized boolean next() throws SQLException
    {
        if (hasMoreRows()) {
            rowNumber++;
            return true;
        } else {
            return false;
        }
    }

    public boolean previous() throws SQLException
    {
        if (rowNumber > 0) {
            rowNumber--;
        }
        return rowNumber > 0;
    }

    public boolean relative(int count) throws SQLException
    {
        rowNumber += count;
        return rowNumber > 0 && rowNumber <= rows.length;
    }

    public void setFetchDirection(int direction) throws SQLException
    {
        if (direction == FETCH_FORWARD || direction == FETCH_REVERSE || direction == FETCH_UNKNOWN) {
            if ((getType() == TYPE_FORWARD_ONLY) && (direction != FETCH_FORWARD)) {
                throw new SQLSyntaxErrorException("Illegal direction : " + direction);
            }
            fetchDirection = direction;
        } else {
            throw new SQLSyntaxErrorException("Not supported fetch direction: " + direction);
        }
    }

    public void setFetchSize(int size) throws SQLException
    {
        // ignore
    }

    public <T> T unwrap(Class<T> clazz) throws SQLException
    {
        throw new SQLFeatureNotSupportedException("Can't unwrap " + clazz.getName());
    }

    public boolean wasNull() throws SQLException
    {
        return wasNull;
    }

    /**
     * RSMD implementation.  The metadata returned refers to the column
     * values, not the column names.
     */
    class MetaData implements ResultSetMetaData {
        public String getCatalogName(int column) throws SQLException
        {
            checkIndex(column);
            return "";
        }

        public String getColumnClassName(int column) throws SQLException
        {
            checkIndex(column);
            Object value = rows[rowNumber - 1][column - 1];
            return value == null ? Object.class.getName() : value.getClass().getName();
        }

        public int getColumnCount() throws SQLException
        {
            return columns.length;
        }

        public int getColumnDisplaySize(int column) throws SQLException
        {
            checkIndex(column);
            return 0;
        }

        public String getColumnLabel(int column) throws SQLException
        {
            checkIndex(column);
            return getColumnName(column);
        }

        public String getColumnName(int column) throws SQLException
        {
            checkIndex(column);
            return columns[column - 1].getColumnName();
        }

        public int getColumnType(int column) throws SQLException
        {
            checkIndex(column);
            return columns[column - 1].getValueType();
        }

        // Spec says "database specific type name". For Cassandra this means the abstract type.
        public String getColumnTypeName(int column) throws SQLException
        {
            Object value = rows[rowNumber - 1][column - 1];
            return value == null ? Object.class.getName() : value.getClass().getSimpleName();
        }

        public int getPrecision(int column) throws SQLException
        {
            checkIndex(column);
            return 0;
        }

        public int getScale(int column) throws SQLException
        {
            checkIndex(column);
            return 0;
        }

        public String getSchemaName(int column) throws SQLException
        {
            checkIndex(column);
            return null;
        }

        public String getTableName(int column) throws SQLException
        {
            return null;
        }

        public boolean isAutoIncrement(int column) throws SQLException
        {
            checkIndex(column);
            return false;
        }

        public boolean isCaseSensitive(int column) throws SQLException
        {
            checkIndex(column);
            return true;
        }

        public boolean isCurrency(int column) throws SQLException
        {
            checkIndex(column);
            return false;
        }

        public boolean isDefinitelyWritable(int column) throws SQLException
        {
            checkIndex(column);
            return isWritable(column);
        }

        /**
         * absence is the equivalent of null in Cassandra
         */
        public int isNullable(int column) throws SQLException
        {
            checkIndex(column);
            return ResultSetMetaData.columnNullable;
        }

        public boolean isReadOnly(int column) throws SQLException
        {
            checkIndex(column);
            return true;
        }

        public boolean isSearchable(int column) throws SQLException
        {
            checkIndex(column);
            return false;
        }

        public boolean isSigned(int column) throws SQLException
        {
            checkIndex(column);
            return false;
        }

        public boolean isWrapperFor(Class<?> clazz) throws SQLException
        {
            return false;
        }

        public boolean isWritable(int column) throws SQLException
        {
            checkIndex(column);
            return column > 0;
        }

        public <T> T unwrap(Class<T> clazz) throws SQLException
        {
            throw new SQLFeatureNotSupportedException("Can't unwrap " + clazz.getName());
        }
    }
}
