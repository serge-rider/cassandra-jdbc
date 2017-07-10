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

package org.jkiss.jdbc.cassandra;

import org.apache.cassandra.thrift.CfDef;
import org.apache.cassandra.thrift.Compression;

import java.io.ByteArrayOutputStream;
import java.lang.reflect.Constructor;
import java.net.InetAddress;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.Charset;
import java.sql.SQLException;
import java.sql.SQLNonTransientConnectionException;
import java.sql.SQLSyntaxErrorException;
import java.util.Arrays;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.zip.Deflater;

/**
 * A set of static utility methods used by the JDBC Suite, and various default values and error message strings
 * that can be shared across classes.
 */
public class CassandraUtils {
    private static final Pattern KEYSPACE_PATTERN = Pattern.compile("USE (\\w+);?", Pattern.CASE_INSENSITIVE | Pattern.MULTILINE);
    private static final Pattern SELECT_PATTERN = Pattern.compile("SELECT\\s+.*FROM\\s+[\\w+\\.]+", Pattern.CASE_INSENSITIVE | Pattern.MULTILINE);
    private static final Pattern SELECT_COUNT_PATTERN = Pattern.compile("SELECT\\s+COUNT\\([^\\)]+\\).*", Pattern.CASE_INSENSITIVE | Pattern.MULTILINE);
    private static final Pattern SELECT_ALL_PATTERN = Pattern.compile("SELECT\\s+\\*.*", Pattern.CASE_INSENSITIVE | Pattern.MULTILINE);
    private static final Pattern SELECT_DELETE_PATTERN = Pattern.compile("(?:SELECT|DELETE)\\s+.+FROM\\s+([\\w+\\.]+).*", Pattern.CASE_INSENSITIVE | Pattern.MULTILINE);
    private static final Pattern UPDATE_PATTERN = Pattern.compile("UPDATE\\s+([\\w+\\.]+)\\s+.*", Pattern.CASE_INSENSITIVE);

    public static final ByteBuffer EMPTY_BYTE_BUFFER = ByteBuffer.wrap(new byte[0]);

    public static final Charset UTF_8;
    public static final Charset US_ASCII;

    private static final char[] byteToChar = new char[16];
    private final static byte[] charToByte = new byte[256];
    private static final Constructor<String> stringConstructor = getProtectedConstructor(String.class, int.class, int.class, char[].class);
    private static InetAddress localInetAddress;

    static {
        UTF_8 = Charset.forName("utf-8");
        US_ASCII = Charset.forName("us-ascii");

        for (char c = 0; c < charToByte.length; ++c) {
            if (c >= '0' && c <= '9')
                charToByte[c] = (byte) (c - '0');
            else if (c >= 'A' && c <= 'F')
                charToByte[c] = (byte) (c - 'A' + 10);
            else if (c >= 'a' && c <= 'f')
                charToByte[c] = (byte) (c - 'a' + 10);
            else
                charToByte[c] = (byte) -1;
        }

        for (int i = 0; i < 16; ++i) {
            byteToChar[i] = Integer.toHexString(i).charAt(0);
        }
    }

    /**
     * Use the Compression object method to deflate the query string
     *
     * @param queryStr    An un-compressed CQL query string
     * @param compression The compression object
     * @return A compressed string
     */
    public static ByteBuffer compressQuery(String queryStr, Compression compression)
    {
        byte[] data = queryStr.getBytes(CassandraUtils.UTF_8);
        Deflater compressor = new Deflater();
        compressor.setInput(data);
        compressor.finish();

        ByteArrayOutputStream byteArray = new ByteArrayOutputStream();
        byte[] buffer = new byte[1024];

        try {
            while (!compressor.finished()) {
                int size = compressor.deflate(buffer);
                byteArray.write(buffer, 0, size);
            }
        } finally {
            compressor.end(); //clean up after the Deflater
        }

        //log.trace("Compressed query statement {} bytes in length to {} bytes", data.length, byteArray.size());

        return ByteBuffer.wrap(byteArray.toByteArray());
    }

    /**
     * Parse a URL for the Cassandra JDBC Driver
     * <p/>
     * The URL must start with the Protocol: "jdbc:jkiss:cassandra:"
     * The URI part(the "Subname") must contain a host and an optional port and optional keyspace name
     * ie. "//localhost:9160/Test1"
     *
     * @param url The full JDBC URL to be parsed
     * @return A list of properties that were parsed from the Subname
     * @throws SQLException
     */
    public static final Properties parseURL(String url) throws SQLException
    {
        Properties props = new Properties();

        if (!(url == null)) {
            props.setProperty(CassandraConstants.PROP_PORT_NUMBER, "" + CassandraConstants.DEFAULT_PORT);
            String rawUri = url.substring(CassandraConstants.PROTOCOL.length());
            URI uri = null;
            try {
                uri = new URI(rawUri);
            } catch (URISyntaxException e) {
                throw new SQLSyntaxErrorException(e);
            }

            String host = uri.getHost();
            if (host == null) throw new SQLNonTransientConnectionException(ErrorMessages.HOST_IN_URL);
            props.setProperty(CassandraConstants.PROP_SERVER_NAME, host);

            int port = uri.getPort() >= 0 ? uri.getPort() : CassandraConstants.DEFAULT_PORT;
            props.setProperty(CassandraConstants.PROP_PORT_NUMBER, "" + port);

            String keyspace = uri.getPath();
            if ((keyspace != null) && (!keyspace.isEmpty())) {
                if (keyspace.startsWith("/")) keyspace = keyspace.substring(1);
                if (!keyspace.matches("[a-zA-Z]\\w+"))
                    throw new SQLNonTransientConnectionException(String.format(ErrorMessages.BAD_KEYSPACE, keyspace));
                props.setProperty(CassandraConstants.PROP_DATABASE_NAME, keyspace);
            }

            if (uri.getUserInfo() != null)
                throw new SQLNonTransientConnectionException(ErrorMessages.URI_IS_SIMPLE);

            String query = uri.getQuery();
            if ((query != null) && (!query.isEmpty())) {
                String[] items = query.split("&");
                if (items.length != 1) throw new SQLNonTransientConnectionException(ErrorMessages.URI_IS_SIMPLE);

                String[] option = query.split("=");
                if (!option[0].equalsIgnoreCase("version"))
                    throw new SQLNonTransientConnectionException(ErrorMessages.NOT_OPTION);
                if (option.length != 2) throw new SQLNonTransientConnectionException(ErrorMessages.NOT_OPTION);
                props.setProperty(CassandraConstants.PROP_CQL_VERSION, option[1]);
            }
        }

        //if (log.isTraceEnabled()) log.trace("URL : '{}' parses to: {}", url, props);

        return props;
    }

    /**
     * Create a "Subname" portion of a JDBC URL from properties.
     *
     * @param props A Properties file containing all the properties to be considered.
     * @return A constructed "Subname" portion of a JDBC URL in the form of a CLI (ie: //myhost:9160/Test1?version=3.0.0 )
     * @throws SQLException
     */
    public static URI getConnectionURI(Properties props) throws SQLException
    {
        // make keyspace always start with a "/" for URI
        String keyspace = props.getProperty(CassandraConstants.PROP_DATABASE_NAME);

        // if keyspace is null then do not bother ...
        if (keyspace != null)
            if (!keyspace.startsWith("/")) keyspace = "/" + keyspace;

        String host = props.getProperty(CassandraConstants.PROP_SERVER_NAME);
        if (host == null) throw new SQLNonTransientConnectionException(ErrorMessages.HOST_REQUIRED);

        String version = (props.getProperty(CassandraConstants.PROP_CQL_VERSION) == null) ? null : "version=" + props.getProperty(CassandraConstants.PROP_CQL_VERSION);

        // construct a valid URI from parts... 
        try {
            return new URI(
                null,
                null,
                host,
                props.getProperty(CassandraConstants.PROP_PORT_NUMBER) == null ? CassandraConstants.DEFAULT_PORT : Integer.parseInt(props.getProperty(CassandraConstants.PROP_PORT_NUMBER)),
                keyspace,
                version,
                null);
        } catch (Exception e) {
            throw new SQLNonTransientConnectionException(e);
        }
    }

    public static boolean isSelectQuery(String cql)
    {
        return SELECT_PATTERN.matcher(cql).matches();
    }

    public static boolean isSelectCountQuery(String cql)
    {
        return SELECT_COUNT_PATTERN.matcher(cql).matches();
    }

    public static boolean isSelectAllQuery(String cql)
    {
        return SELECT_ALL_PATTERN.matcher(cql).matches();
    }

    /**
    * Determine the current keyspace by inspecting the CQL string to see if a USE statement is provided; which would change the keyspace.
    *
    * @param cql     A CQL query string
    * @param current The current keyspace stored as state in the connection
    * @return the provided keyspace name or the keyspace from the contents of the CQL string
    */
    public static String determineCurrentKeyspace(String cql, String current)
    {
        String ks = null;
        Matcher isKeyspace = KEYSPACE_PATTERN.matcher(cql);
        if (isKeyspace.matches()) {
            ks = isKeyspace.group(1);
        }
        if (ks == null) {
            Matcher isSelect = SELECT_DELETE_PATTERN.matcher(cql);
            if (isSelect.matches()) ks = determineCurrentSource(isSelect.group(1), true);
        }
        if (ks == null) {
            Matcher isUpdate = UPDATE_PATTERN.matcher(cql);
            if (isUpdate.matches()) ks = determineCurrentSource(isUpdate.group(1), true);
        }
        return ks != null ? ks : current;
    }

    /**
     * Determine the current column family by inspecting the CQL to find a CF reference.
     *
     * @param cql A CQL query string
     * @return The column family name from the contents of the CQL string or null in none was found
     */
    public static String determineCurrentColumnFamily(String cql)
    {
        Matcher isSelect = SELECT_DELETE_PATTERN.matcher(cql);
        if (isSelect.matches()) return determineCurrentSource(isSelect.group(1), false);
        Matcher isUpdate = UPDATE_PATTERN.matcher(cql);
        if (isUpdate.matches()) return determineCurrentSource(isUpdate.group(1), false);
        return null;
    }

    public static String modifyQueryLimits(String queryStr, int maxRows)
    {
        if (maxRows > 0 && CassandraUtils.isSelectQuery(queryStr) && !CassandraUtils.isSelectCountQuery(queryStr)) {
            queryStr += " LIMIT " + maxRows;
        }
        return queryStr;
    }

    public static String determineCurrentSource(String source, boolean keyspace)
    {
        int divPos = source.indexOf('.');
        if (keyspace) {
            if (divPos == -1) {
                // No keyspace
                return null;
            } else {
                return source.substring(0, divPos);
            }
        } else {
            if (divPos == -1) {
                return source;
            } else {
                return source.substring(divPos + 1);
            }
        }
    }

    public static boolean matchesPattern(String name, String patternStr)
    {
        Pattern pattern = Pattern.compile(
            patternStr.replace("%", ".*").replace("_", "."),
            Pattern.CASE_INSENSITIVE);
        return pattern.matcher(name).matches();
    }

    public static ByteBuffer bytes(String s)
    {
        return ByteBuffer.wrap(s.getBytes(UTF_8));
    }

    public static ByteBuffer bytes(String s, Charset charset)
    {
        return ByteBuffer.wrap(s.getBytes(charset));
    }

    public static String string(ByteBuffer buffer) throws CharacterCodingException
    {
        return string(buffer, UTF_8);
    }

    public static String string(ByteBuffer buffer, Charset charset) throws CharacterCodingException
    {
        return charset.newDecoder().decode(buffer.duplicate()).toString();
    }

    public static String string(byte[] bytes)
    {
        try {
            return new String(bytes, UTF_8);
        } catch (Exception e) {
            // never happens
            return new String(bytes);
        }
    }

    public static ByteBuffer cloneByteBuffer(ByteBuffer buffer)
    {
        assert buffer != null;

        if (buffer.remaining() == 0)
            return EMPTY_BYTE_BUFFER;

        ByteBuffer clone = ByteBuffer.allocate(buffer.remaining());

        if (buffer.hasArray()) {
            System.arraycopy(buffer.array(), buffer.arrayOffset() + buffer.position(), clone.array(), 0, buffer.remaining());
        } else {
            clone.put(buffer.duplicate());
            clone.flip();
        }

        return clone;
    }

    public static String wrapCharArray(char[] c)
    {
        if (c == null)
            return null;

        String s = null;

        if (stringConstructor != null) {
            try {
                s = stringConstructor.newInstance(0, c.length, c);
            } catch (Exception e) {
                // Swallowing as we'll just use a copying constructor
            }
        }
        return s == null ? new String(c) : s;
    }

    public static String bytesToHex(ByteBuffer bytes)
    {
        final int offset = bytes.position();
        final int size = bytes.remaining();
        final char[] c = new char[size * 2];
        for (int i = 0; i < size; i++) {
            final int bint = bytes.get(i + offset);
            c[i * 2] = byteToChar[(bint & 0xf0) >> 4];
            c[1 + i * 2] = byteToChar[bint & 0x0f];
        }
        return wrapCharArray(c);
    }

    public static String bytesToHex(byte[] bytes)
    {
        final int size = bytes.length;
        final char[] c = new char[size * 2];
        for (int i = 0; i < size; i++) {
            final int bint = bytes[i];
            c[i * 2] = byteToChar[(bint & 0xf0) >> 4];
            c[1 + i * 2] = byteToChar[bint & 0x0f];
        }
        return wrapCharArray(c);
    }

    public static byte[] getArray(ByteBuffer buffer)
    {
        int length = buffer.remaining();

        if (buffer.hasArray()) {
            int boff = buffer.arrayOffset() + buffer.position();
            if (boff == 0 && length == buffer.array().length)
                return buffer.array();
            else
                return Arrays.copyOfRange(buffer.array(), boff, boff + length);
        }
        // else, DirectByteBuffer.get() is the fastest route
        byte[] bytes = new byte[length];
        buffer.duplicate().get(bytes);

        return bytes;
    }

    /**
     * Convert a byte buffer to an integer.
     * Does not change the byte buffer position.
     *
     * @param bytes byte buffer to convert to integer
     * @return int representation of the byte buffer
     */
    public static int toInt(ByteBuffer bytes)
    {
        return bytes.getInt(bytes.position());
    }

    public static long toLong(ByteBuffer bytes)
    {
        return bytes.getLong(bytes.position());
    }

    public static float toFloat(ByteBuffer bytes)
    {
        return bytes.getFloat(bytes.position());
    }

    public static double toDouble(ByteBuffer bytes)
    {
        return bytes.getDouble(bytes.position());
    }

    public static ByteBuffer bytes(int i)
    {
        return ByteBuffer.allocate(4).putInt(0, i);
    }

    public static ByteBuffer bytes(long n)
    {
        return ByteBuffer.allocate(8).putLong(0, n);
    }

    public static ByteBuffer bytes(float f)
    {
        return ByteBuffer.allocate(4).putFloat(0, f);
    }

    public static ByteBuffer bytes(double d)
    {
        return ByteBuffer.allocate(8).putDouble(0, d);
    }

    public static Constructor getProtectedConstructor(Class klass, Class... paramTypes)
    {
        Constructor c;
        try {
            c = klass.getDeclaredConstructor(paramTypes);
            c.setAccessible(true);
            return c;
        } catch (Exception e) {
            return null;
        }
    }

    public static InetAddress getLocalAddress()
    {
        if (localInetAddress == null)
            try {
                localInetAddress = InetAddress.getLocalHost();
            } catch (UnknownHostException e) {
                throw new RuntimeException(e);
            }
        return localInetAddress;
    }

    public static byte[] getRawKeyAlias(CfDef cf) throws CharacterCodingException
    {
        byte[] keyNameBuffer = cf.getKey_alias();
        return keyNameBuffer == null ? CassandraConstants.DEFAULT_KEY_ALIAS.getBytes() : keyNameBuffer;
    }

    public static String getKeyAlias(CfDef cf) throws CharacterCodingException
    {
        ByteBuffer keyNameBuffer = cf.bufferForKey_alias();
        return keyNameBuffer == null ? CassandraConstants.DEFAULT_KEY_ALIAS : string(keyNameBuffer);
    }

}
