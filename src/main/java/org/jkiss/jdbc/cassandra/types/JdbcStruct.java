package org.jkiss.jdbc.cassandra.types;

import org.jkiss.jdbc.cassandra.CassandraStruct;

import java.nio.ByteBuffer;
import java.sql.Types;

/**
 * Cassandra struct.
 * Contains row columns
 */
public class JdbcStruct extends AbstractJdbcType<CassandraStruct> {

    public static final JdbcStruct instance = new JdbcStruct();

    @Override
    public boolean isCaseSensitive()
    {
        return false;
    }

    @Override
    public int getScale(CassandraStruct obj)
    {
        return 0;
    }

    @Override
    public int getPrecision(CassandraStruct obj)
    {
        return 0;
    }

    @Override
    public boolean isCurrency()
    {
        return false;
    }

    @Override
    public boolean isSigned()
    {
        return false;
    }

    @Override
    public String toString(CassandraStruct obj)
    {
        return obj.toString();
    }

    @Override
    public boolean needsQuotes()
    {
        return false;
    }

    @Override
    public String getString(ByteBuffer bytes)
    {
        return null;
    }

    @Override
    public Class<CassandraStruct> getType()
    {
        return CassandraStruct.class;
    }

    @Override
    public int getJdbcType()
    {
        return Types.STRUCT;
    }

    @Override
    public CassandraStruct compose(ByteBuffer bytes)
    {
        return null;
    }

    @Override
    public ByteBuffer decompose(CassandraStruct obj)
    {
        return null;
    }

}
