package org.jkiss.jdbc.cassandra.types;
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

import org.jkiss.jdbc.cassandra.CassandraUtils;

import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.sql.Types;

public class JdbcBytes extends AbstractJdbcType<byte[]> {
    public static final JdbcBytes instance = new JdbcBytes();

    JdbcBytes()
    {
    }

    public boolean isCaseSensitive()
    {
        return false;
    }

    public int getScale(byte[] obj)
    {
        return -1;
    }

    public int getPrecision(byte[] obj)
    {
        return -1;
    }

    public boolean isCurrency()
    {
        return false;
    }

    public boolean isSigned()
    {
        return false;
    }

    public String toString(byte[] obj)
    {
        return CassandraUtils.bytesToHex(ByteBuffer.wrap(obj));
    }

    public boolean needsQuotes()
    {
        return true;
    }

    public String getString(ByteBuffer bytes)
    {
        try {
            return CassandraUtils.string(bytes);
        } catch (CharacterCodingException e) {
            return "";
        }
    }

    public Class<byte[]> getType()
    {
        return byte[].class;
    }

    public int getJdbcType()
    {
        return Types.BINARY;
    }

    public byte[] compose(ByteBuffer bytes)
    {
        byte data[] = new byte[bytes.remaining()];
        bytes.duplicate().get(data);
        return data;
    }

    public ByteBuffer decompose(byte[] value)
    {
        return ByteBuffer.wrap(value);
    }
}
