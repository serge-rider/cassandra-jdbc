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


import org.apache.cassandra.thrift.Column;
import org.apache.cassandra.thrift.ColumnDef;
import org.jkiss.jdbc.cassandra.types.*;

import java.nio.ByteBuffer;


public class TypedColumn {
    private final Column rawColumn;

    // we cache the frequently-accessed forms: java object for value, String for name.
    // Note that {N|V}.toString() isn't always the same as Type.getString
    // (a good example is byte buffers).
    private final Object value;
    private final String nameString;
    private final AbstractJdbcType<?> nameType, valueType;

    public TypedColumn(Column column, AbstractJdbcType<?> nameType, AbstractJdbcType<?> valueType)
    {
        rawColumn = column;
        this.value = (column.value == null || !column.value.hasRemaining()) ? null : valueType.compose(column.value);
        nameString = nameType.getString(column.name);
        this.nameType = nameType;
        this.valueType = valueType;
    }

    public TypedColumn(CassandraStruct row)
    {
        this.rawColumn = null;
        this.value = row;
        this.nameString = CassandraConstants.ROW_COLUMN_NAME;
        this.nameType = JdbcAscii.instance;
        this.valueType = JdbcStruct.instance;
    }

    public TypedColumn(ColumnDef columnDef)
    {
        this.rawColumn = null;
        this.value = null;
        this.nameString = CassandraUtils.string(columnDef.getName());
        this.nameType = JdbcUTF8.instance;
        this.valueType = TypesMap.getTypeForComparator(columnDef.getValidation_class());
    }

    public TypedColumn(byte[] name, byte[] value, AbstractJdbcType<?> nameType, AbstractJdbcType<?> valueType)
    {
        this.rawColumn = null;
        this.value = valueType.compose(ByteBuffer.wrap(value));
        this.nameString = CassandraUtils.string(name);
        this.nameType = nameType;
        this.valueType = valueType;
    }

    public Column getRawColumn()
    {
        return rawColumn;
    }

    public Integer getTtl()
    {
        return rawColumn == null ? null : rawColumn.getTtl();
    }

    public Long getTimestamp()
    {
        return rawColumn == null ? null : rawColumn.getTimestamp();
    }

    public Object getValue()
    {
        return value;
    }

    public String getNameString()
    {
        return nameString;
    }

    public String getValueString()
    {
        if (rawColumn == null) {
            return value == null ? null : value.toString();
        } else {
            return rawColumn.value == null ? null : valueType.getString(rawColumn.value);
        }
    }

    public AbstractJdbcType getNameType()
    {
        return nameType;
    }

    public AbstractJdbcType getValueType()
    {
        return valueType;
    }

    public String toString()
    {
        return String.format("TypedColumn [value=%s, nameString=%s, nameType=%s, valueType=%s]",
            value,
            nameString,
            nameType,
            valueType);
    }

}
