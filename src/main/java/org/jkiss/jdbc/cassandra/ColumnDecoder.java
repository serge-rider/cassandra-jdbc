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

import org.apache.cassandra.thrift.CfDef;
import org.apache.cassandra.thrift.Column;
import org.apache.cassandra.thrift.ColumnDef;
import org.apache.cassandra.thrift.KsDef;
import org.jkiss.jdbc.cassandra.types.AbstractJdbcType;
import org.jkiss.jdbc.cassandra.types.JdbcAscii;
import org.jkiss.jdbc.cassandra.types.TypesMap;

import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.sql.SQLNonTransientException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;

/**
 * Decodes columns from bytes into instances of their respective expected types.
 */
class ColumnDecoder {
    public final static ByteBuffer DEFAULT_KEY_NAME = CassandraUtils.bytes("KEY");
    //private static final Logger logger = LoggerFactory.getLogger(ColumnDecoder.class);

    private class CFamMeta {
        String comparator;
        String defaultValidator;
        ByteBuffer keyAlias;
        String keyValidator;
        Map<ByteBuffer, String> columnMeta = new HashMap<ByteBuffer, String>();

        private CFamMeta(CfDef cf)
        {
            comparator = cf.getComparator_type();
            defaultValidator = cf.getDefault_validation_class();
            keyAlias = cf.key_alias;
            keyValidator = cf.getKey_validation_class();

            for (ColumnDef colDef : cf.getColumn_metadata())
                columnMeta.put(colDef.name, colDef.getValidation_class());
        }

        public String toString()
        {
            return String.format("CFamMeta(comparator=%s, defaultValidator=%s, keyAlias=%s, keyValidator=%s, columnMeta=%s)",
                comparator,
                defaultValidator,
                keyAlias,
                keyValidator,
                columnMeta);
        }
    }

    private final Map<String, CFamMeta> metadata = new HashMap<String, CFamMeta>();

    /**
     * is specific per set of keyspace definitions.
     */
    public ColumnDecoder(List<KsDef> defs)
    {
        for (KsDef ks : defs)
            for (CfDef cf : ks.getCf_defs())
                metadata.put(ks.getName() + "." + cf.getName(), new CFamMeta(cf));
    }

    protected AbstractJdbcType<?> getComparator(String keyspace, String columnFamily)
    {
        CFamMeta cf = metadata.get(String.format("%s.%s", keyspace, columnFamily));
        AbstractJdbcType<?> type = (cf != null) ? TypesMap.getTypeForComparator(cf.comparator) : null;
        return (type == null) ? null : type;
    }

    protected AbstractJdbcType<?> getDefaultValidator(String keyspace, String columnFamily)
    {
        CFamMeta cf = metadata.get(keyspace + "." + columnFamily);
        AbstractJdbcType<?> type = (cf != null) ? TypesMap.getTypeForComparator(cf.defaultValidator) : null;
        return (type == null) ? null : type;
    }

    private AbstractJdbcType<?> getNameType(String keyspace, String columnFamily, ByteBuffer name)
    {
        CFamMeta cf = metadata.get(keyspace + "." + columnFamily);
        try {
            if (CassandraUtils.string(name).equalsIgnoreCase(CassandraUtils.string(cf.keyAlias)))
                return JdbcAscii.instance;
        } catch (CharacterCodingException e) {
            // not be the key name
        }
        return TypesMap.getTypeForComparator(cf.comparator);
    }

    private AbstractJdbcType<?> getValueType(String keyspace, String columnFamily, ByteBuffer name)
    {
        CFamMeta cf = metadata.get(keyspace + "." + columnFamily);
        if (cf == null)
            return null;

        try {
            if (CassandraUtils.string(name).equalsIgnoreCase(CassandraUtils.string(cf.keyAlias)))
                return TypesMap.getTypeForComparator(cf.keyValidator);
        } catch (CharacterCodingException e) {
            // not be the key name
        }

        AbstractJdbcType<?> type = TypesMap.getTypeForComparator(cf.columnMeta.get(name));
        return (type != null) ? type : TypesMap.getTypeForComparator(cf.defaultValidator);
    }

    public AbstractJdbcType<?> getKeyValidator(String keyspace, String columnFamily)
    {
        CFamMeta cf = metadata.get(keyspace + "." + columnFamily);
        AbstractJdbcType<?> type = (cf != null) ? TypesMap.getTypeForComparator(cf.keyValidator) : null;
        return (type == null) ? null : type;
    }

    /**
     * uses the AbstractType to map a column name to a string.
     */
    public String colNameAsString(String keyspace, String columnFamily, ByteBuffer name)
    {
        AbstractJdbcType<?> comparator = getNameType(keyspace, columnFamily, name);
        return comparator.getString(name);
    }

    /**
     * constructs a typed column
     */
    public TypedColumn makeCol(String keyspace, String columnFamily, Column column)
    {
        return new TypedColumn(column,
            getNameType(keyspace, columnFamily, column.name),
            getValueType(keyspace, columnFamily, column.name));
    }

    /**
     * constructs a typed column to hold the key
     *
     * @throws SQLNonTransientException
     */
    public TypedColumn makeKeyColumn(String keyspace, String columnFamily, byte[] key) throws SQLNonTransientException
    {
        CFamMeta cf = metadata.get(keyspace + "." + columnFamily);
        if (cf == null)
            throw new SQLNonTransientException(String.format("could not find decoder metadata for: %s.%s",
                keyspace,
                columnFamily));

        Column column = new Column(cf.keyAlias).setValue(key).setTimestamp(-1);
        return new TypedColumn(column,
            getNameType(keyspace, columnFamily, (cf.keyAlias != null) ? cf.keyAlias : DEFAULT_KEY_NAME),
            getValueType(keyspace, columnFamily, (cf.keyAlias != null) ? cf.keyAlias : DEFAULT_KEY_NAME));
    }
}
