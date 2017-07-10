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
import org.apache.cassandra.thrift.ColumnDef;
import org.apache.cassandra.thrift.InvalidRequestException;
import org.apache.cassandra.thrift.KsDef;
import org.apache.thrift.TException;
import org.jkiss.jdbc.cassandra.types.AbstractJdbcType;
import org.jkiss.jdbc.cassandra.types.TypesMap;

import java.nio.charset.CharacterCodingException;
import java.sql.*;
import java.util.*;

class CassandraDatabaseMetaData implements DatabaseMetaData {

    private CassandraConnection connection;
    private List<KsDef> keyspaces;
    private Map<String, String> versions;
    private int majorVersion, minorVersion;
    private boolean showCluster = false;

    private static class TableColumnDef {
        final KsDef keyspace;
        final CfDef columnFamily;
        final String validationClass;
        final String columnName;
        final int position;
        final ColumnDef column;

        private TableColumnDef(KsDef keyspace, CfDef columnFamily, String columnName, String validationClass, int position, ColumnDef column)
        {
            this.keyspace = keyspace;
            this.columnFamily = columnFamily;
            this.validationClass = validationClass;
            this.columnName = columnName;
            this.position = position;
            this.column = column;
        }
    }

    public CassandraDatabaseMetaData(CassandraConnection connection)
    {
        this.connection = connection;
    }

    private void readVersions()
    {
        if (versions != null) {
            return;
        }
        versions = new TreeMap<String, String>();
        try {
            // Read versions
            Statement dbStat = connection.createStatement();
            try {
                ResultSet dbResult = dbStat.executeQuery("SELECT component,version FROM system.Versions");
                while (dbResult.next()) {
                    versions.put(
                        dbResult.getString("component"),
                        dbResult.getString("version")
                    );
                }
            } finally {
                dbStat.close();
            }
        } catch (SQLException e) {
            // ignore
        }
        String buildVersion = versions.get("build");
        if (buildVersion != null) {
            StringTokenizer st = new StringTokenizer(buildVersion, ".");
            majorVersion = Integer.parseInt(st.nextToken());
            if (st.hasMoreTokens()) {
                minorVersion = Integer.parseInt(st.nextToken());
            }
        }
    }

    public boolean isWrapperFor(Class<?> clazz) throws SQLException
    {
        return clazz.isAssignableFrom(getClass());
    }

    public <T> T unwrap(Class<T> clazz) throws SQLException
    {
        if (clazz.isAssignableFrom(getClass())) {
            return clazz.cast(this);
        }
        throw new SQLFeatureNotSupportedException("Can't unwrap from " + clazz.getName());
    }

    public boolean allProceduresAreCallable() throws SQLException
    {
        return false;
    }

    public boolean allTablesAreSelectable() throws SQLException
    {
        return true;
    }

    public boolean autoCommitFailureClosesAllResultSets() throws SQLException
    {
        return false;
    }

    public boolean dataDefinitionCausesTransactionCommit() throws SQLException
    {
        return false;
    }

    public boolean dataDefinitionIgnoredInTransactions() throws SQLException
    {
        return false;
    }

    public boolean deletesAreDetected(int arg0) throws SQLException
    {
        return false;
    }

    public boolean doesMaxRowSizeIncludeBlobs() throws SQLException
    {
        return false;
    }

    @Override
    public ResultSet getBestRowIdentifier(String catalog, String schema, String table, int scope, boolean nullable) throws SQLException
    {
        return null;
    }

    @Override
    public ResultSet getAttributes(String catalog, String schemaPattern, String typeNamePattern, String attributeNamePattern) throws SQLException
    {
        return null;
    }

    public String getCatalogSeparator() throws SQLException
    {
        return "";
    }

    public String getCatalogTerm() throws SQLException
    {
        return "cluster";
    }

    public ResultSet getCatalogs() throws SQLException
    {
        if (!showCluster) {
            return new LocalResultSet();
        }
        try {
            String cluster = connection.getClient().describe_cluster_name();
            LocalColumn[] columns = new LocalColumn[]{
                new LocalColumn("TABLE_CAT", Types.VARCHAR),
                new LocalColumn("VERSION", Types.VARCHAR),
                new LocalColumn("SNITCH", Types.VARCHAR),
                new LocalColumn("PARTITIONER", Types.VARCHAR),
                new LocalColumn("TOKEN_MAP", Types.OTHER),
            };
            Object[][] rows = new Object[1][columns.length];
            rows[0][0] = cluster;
            rows[0][1] = connection.getClient().describe_version();
            rows[0][2] = connection.getClient().describe_snitch();
            rows[0][3] = connection.getClient().describe_partitioner();
            rows[0][4] = connection.getClient().describe_token_map();
            return new LocalResultSet(null, columns, rows);
        } catch (Exception e) {
            throw new SQLException(e);
        }
    }

    private List<KsDef> readKeyspaces() throws TException, InvalidRequestException
    {
        if (keyspaces == null) {
            keyspaces = connection.getClient().describe_keyspaces();
        }
        return keyspaces;
    }

    public ResultSet getClientInfoProperties() throws SQLException
    {
        return new LocalResultSet();
    }

    @Override
    public ResultSet getColumns(String catalog, String schemaPattern, String tableNamePattern, String columnNamePattern) throws SQLException
    {
        try {
            List<TableColumnDef> cfColumns = getColumnDefs(catalog, schemaPattern, tableNamePattern);
            LocalColumn[] columns = new LocalColumn[]{
                new LocalColumn("TABLE_CATALOG", Types.VARCHAR),
                new LocalColumn("TABLE_SCHEM", Types.VARCHAR),
                new LocalColumn("TABLE_NAME", Types.VARCHAR),
                new LocalColumn("COLUMN_NAME", Types.VARCHAR),
                new LocalColumn("DATA_TYPE", Types.VARCHAR),
                new LocalColumn("TYPE_NAME", Types.VARCHAR),
                new LocalColumn("COLUMN_SIZE", Types.INTEGER),
                new LocalColumn("BUFFER_LENGTH", Types.INTEGER),
                new LocalColumn("DECIMAL_DIGITS", Types.INTEGER),
                new LocalColumn("NUM_PREC_RADIX", Types.INTEGER),
                new LocalColumn("NULLABLE", Types.INTEGER),
                new LocalColumn("REMARKS", Types.VARCHAR),
                new LocalColumn("COLUMN_DEF", Types.VARCHAR),
                new LocalColumn("CHAR_OCTET_LENGTH", Types.INTEGER),
                new LocalColumn("ORDINAL_POSITION", Types.INTEGER),
                new LocalColumn("IS_NULLABLE", Types.VARCHAR),
                new LocalColumn("SCOPE_CATLOG", Types.VARCHAR),
                new LocalColumn("SCOPE_SCHEMA", Types.VARCHAR),
                new LocalColumn("SCOPE_TABLE", Types.VARCHAR),
                new LocalColumn("SOURCE_DATA_TYPE", Types.VARCHAR),
                new LocalColumn("IS_AUTOINCREMENT", Types.INTEGER),
                new LocalColumn("INDEX_NAME", Types.VARCHAR),
                new LocalColumn("INDEX_TYPE", Types.VARCHAR),
                new LocalColumn("INDEX_OPTIONS", Types.OTHER),
            };
            Object[][] rows = new Object[cfColumns.size()][columns.length];
            for (int i = 0; i < cfColumns.size(); i++) {
                TableColumnDef col = cfColumns.get(i);
                String columnType = col.validationClass;
                AbstractJdbcType<?> type = TypesMap.getTypeForComparator(columnType);
                rows[i][0] = catalog;
                rows[i][1] = col.keyspace.getName();
                rows[i][2] = col.columnFamily.getName();
                rows[i][3] = col.columnName;
                rows[i][4] = type == null ? Types.BINARY : type.getJdbcType();
                rows[i][5] = columnType;
                rows[i][6] = Integer.MAX_VALUE;
                rows[i][7] = null;
                rows[i][8] = null;
                rows[i][9] = null;
                rows[i][10] = col.column == null ? DatabaseMetaData.columnNoNulls : DatabaseMetaData.columnNullable;
                rows[i][11] = null;
                rows[i][12] = null;
                rows[i][13] = null;
                rows[i][14] = col.position;
                rows[i][15] = col.column == null ? "YES" : "NO";
                rows[i][16] = null;
                rows[i][17] = null;
                rows[i][18] = null;
                rows[i][19] = null;
                rows[i][20] = "NO";
                rows[i][21] = col.column == null ? null : col.column.getIndex_name();
                rows[i][22] = col.column == null ? null : col.column.getIndex_type();
                rows[i][23] = col.column == null ? null : col.column.getIndex_options();
            }
            return new LocalResultSet(null, columns, rows);
        } catch (Exception e) {
            throw new SQLException(e);
        }
    }

    @Override
    public ResultSet getColumnPrivileges(String catalog, String schema, String table, String columnNamePattern) throws SQLException
    {
        throw new SQLFeatureNotSupportedException();
    }

    public Connection getConnection() throws SQLException
    {
        return connection;
    }

    @Override
    public ResultSet getCrossReference(String parentCatalog, String parentSchema, String parentTable, String foreignCatalog, String foreignSchema, String foreignTable) throws SQLException
    {
        throw new SQLFeatureNotSupportedException();
    }

    public int getDatabaseMajorVersion() throws SQLException
    {
        readVersions();
        return majorVersion;
    }

    public int getDatabaseMinorVersion() throws SQLException
    {
        readVersions();
        return minorVersion;
    }

    public String getDatabaseProductName() throws SQLException
    {
        readVersions();
        return CassandraConstants.DB_PRODUCT_NAME;
    }

    public String getDatabaseProductVersion() throws SQLException
    {
        readVersions();
        return versions.toString();
    }

    public int getDefaultTransactionIsolation() throws SQLException
    {
        return Connection.TRANSACTION_NONE;
    }

    public int getDriverMajorVersion()
    {
        return CassandraConstants.DRIVER_MAJOR_VERSION;
    }

    public int getDriverMinorVersion()
    {
        return CassandraConstants.DRIVER_MINOR_VERSION;
    }

    public String getDriverName() throws SQLException
    {
        return CassandraConstants.DRIVER_NAME;
    }

    public String getDriverVersion() throws SQLException
    {
        return CassandraConstants.DRIVER_MAJOR_VERSION + "." + CassandraConstants.DRIVER_MINOR_VERSION + "." + CassandraConstants.DRIVER_PATCH_VERSION;
    }

    @Override
    public ResultSet getExportedKeys(String catalog, String schema, String table) throws SQLException
    {
        throw new SQLFeatureNotSupportedException();
    }

    public String getExtraNameCharacters() throws SQLException
    {
        return "";
    }

    public ResultSet getFunctionColumns(String arg0, String arg1, String arg2, String arg3) throws SQLException
    {
        return new LocalResultSet();
    }

    public ResultSet getFunctions(String arg0, String arg1, String arg2) throws SQLException
    {
        return new LocalResultSet();
    }

    public String getIdentifierQuoteString() throws SQLException
    {
        return "'";
    }

    public ResultSet getImportedKeys(String arg0, String arg1, String arg2) throws SQLException
    {
        return new LocalResultSet();
    }

    public ResultSet getIndexInfo(String catalog, String schemaPattern, String tableNamePattern, boolean unique, boolean approximate) throws SQLException
    {
        try {
            List<TableColumnDef> cfColumns = getColumnDefs(catalog, schemaPattern, tableNamePattern);
            for (Iterator<TableColumnDef> iter = cfColumns.iterator(); iter.hasNext(); ) {
                TableColumnDef col = iter.next();
                if (col.column == null || col.column.getIndex_name() == null || col.column.getIndex_name().isEmpty()) {
                    iter.remove();
                }
            }
            LocalColumn[] columns = new LocalColumn[]{
                new LocalColumn("TABLE_CATALOG", Types.VARCHAR),
                new LocalColumn("TABLE_SCHEM", Types.VARCHAR),
                new LocalColumn("TABLE_NAME", Types.VARCHAR),
                new LocalColumn("NON_UNIQUE", Types.VARCHAR),
                new LocalColumn("INDEX_QUALIFIER", Types.VARCHAR),
                new LocalColumn("INDEX_NAME", Types.VARCHAR),
                new LocalColumn("TYPE", Types.INTEGER),
                new LocalColumn("ORDINAL_POSITION", Types.INTEGER),
                new LocalColumn("COLUMN_NAME", Types.INTEGER),
                new LocalColumn("ASC_OR_DESC", Types.INTEGER),
                new LocalColumn("CARDINALITY", Types.INTEGER),
                new LocalColumn("PAGES", Types.VARCHAR),
                new LocalColumn("FILTER_CONDITION", Types.VARCHAR),
            };
            Object[][] rows = new Object[cfColumns.size()][columns.length];
            for (int i = 0; i < cfColumns.size(); i++) {
                TableColumnDef col = cfColumns.get(i);
                rows[i][0] = catalog;
                rows[i][1] = col.keyspace.getName();
                rows[i][2] = col.columnFamily.getName();
                rows[i][3] = true;
                rows[i][4] = null;
                rows[i][5] = col.column.getIndex_name();
                rows[i][6] = col.column.getIndex_type().getValue();
                rows[i][7] = 1;
                rows[i][8] = col.columnName;
                rows[i][9] = "A";
                rows[i][10] = 0;
                rows[i][11] = 0;
                rows[i][12] = 0;
            }
            return new LocalResultSet(null, columns, rows);
        } catch (Exception e) {
            throw new SQLException(e);
        }
    }

    public int getJDBCMajorVersion() throws SQLException
    {
        return 4;
    }

    public int getJDBCMinorVersion() throws SQLException
    {
        return 0;
    }

    public int getMaxBinaryLiteralLength() throws SQLException
    {
        // Cassandra can represent a 2GB value, but CQL has to encode it in hex
        return Integer.MAX_VALUE / 2;
    }

    public int getMaxCatalogNameLength() throws SQLException
    {
        return Short.MAX_VALUE;
    }

    public int getMaxCharLiteralLength() throws SQLException
    {
        return Integer.MAX_VALUE;
    }

    public int getMaxColumnNameLength() throws SQLException
    {
        return Short.MAX_VALUE;
    }

    public int getMaxColumnsInGroupBy() throws SQLException
    {
        return 0;
    }

    public int getMaxColumnsInIndex() throws SQLException
    {
        return 0;
    }

    public int getMaxColumnsInOrderBy() throws SQLException
    {
        return 0;
    }

    public int getMaxColumnsInSelect() throws SQLException
    {
        return 0;
    }

    public int getMaxColumnsInTable() throws SQLException
    {
        return 0;
    }

    public int getMaxConnections() throws SQLException
    {
        return 0;
    }

    public int getMaxCursorNameLength() throws SQLException
    {
        return 0;
    }

    public int getMaxIndexLength() throws SQLException
    {
        return 0;
    }

    public int getMaxProcedureNameLength() throws SQLException
    {
        return 0;
    }

    public int getMaxRowSize() throws SQLException
    {
        return 0;
    }

    public int getMaxSchemaNameLength() throws SQLException
    {
        return 0;
    }

    public int getMaxStatementLength() throws SQLException
    {
        return 0;
    }

    public int getMaxStatements() throws SQLException
    {
        return 0;
    }

    public int getMaxTableNameLength() throws SQLException
    {
        return 0;
    }

    public int getMaxTablesInSelect() throws SQLException
    {
        return 0;
    }

    public int getMaxUserNameLength() throws SQLException
    {
        return 0;
    }

    public String getNumericFunctions() throws SQLException
    {
        return null;
    }

    public ResultSet getPrimaryKeys(String catalog, String schema, String table) throws SQLException
    {
        try {
            String cluster = showCluster ? connection.getClient().describe_cluster_name() : null;
            if (catalog != null && !catalog.isEmpty() && !catalog.equals(cluster)) {
                return new LocalResultSet();
            }
            List<KsDef> keyspaces = readKeyspaces();
            List<TableColumnDef> cfColumns = new ArrayList<TableColumnDef>();
            for (KsDef ks : keyspaces) {
                if (schema != null && !schema.isEmpty() && !schema.equals(ks.getName())) {
                    continue;
                }
                for (CfDef cf : ks.getCf_defs()) {
                    if (table != null && !table.isEmpty() && !table.equals(cf.getName())) {
                        continue;
                    }
                    cfColumns.add(new TableColumnDef(
                        ks,
                        cf,
                        CassandraUtils.getKeyAlias(cf),
                        cf.getKey_validation_class(),
                        1,
                        null));
                }
            }
            LocalColumn[] columns = new LocalColumn[]{
                new LocalColumn("TABLE_CAT", Types.VARCHAR),
                new LocalColumn("TABLE_SCHEM", Types.VARCHAR),
                new LocalColumn("TABLE_NAME", Types.VARCHAR),
                new LocalColumn("COLUMN_NAME", Types.VARCHAR),
                new LocalColumn("KEY_SEQ", Types.INTEGER),
                new LocalColumn("PK_NAME", Types.VARCHAR),
            };
            Object[][] rows = new Object[cfColumns.size()][columns.length];
            for (int i = 0; i < cfColumns.size(); i++) {
                TableColumnDef col = cfColumns.get(i);
                rows[i][0] = cluster;
                rows[i][1] = col.keyspace.getName();
                rows[i][2] = col.columnFamily.getName();
                rows[i][3] = col.columnName;
                rows[i][4] = col.position;
                rows[i][5] = CassandraConstants.DEFAULT_KEY_ALIAS;
            }
            return new LocalResultSet(null, columns, rows);
        } catch (Exception e) {
            throw new SQLException(e);
        }
    }

    public ResultSet getProcedureColumns(String arg0, String arg1, String arg2, String arg3) throws SQLException
    {
        return new LocalResultSet();
    }

    public String getProcedureTerm() throws SQLException
    {
        return null;
    }

    public ResultSet getProcedures(String arg0, String arg1, String arg2) throws SQLException
    {
        return new LocalResultSet();
    }

    public int getResultSetHoldability() throws SQLException
    {
        return CassandraResultSet.DEFAULT_HOLDABILITY;
    }

    public RowIdLifetime getRowIdLifetime() throws SQLException
    {
        return RowIdLifetime.ROWID_VALID_FOREVER;
    }

    public String getSQLKeywords() throws SQLException
    {
        return "";
    }

    public int getSQLStateType() throws SQLException
    {
        return sqlStateSQL;
    }

    public String getSchemaTerm() throws SQLException
    {
        return "keyspace";
    }

    public ResultSet getSchemas() throws SQLException
    {
        return getSchemas(null, null);
    }

    public ResultSet getSchemas(String catalog, String schemaPattern) throws SQLException
    {
        try {
            String cluster = showCluster ? connection.getClient().describe_cluster_name() : null;
            if (catalog != null && !catalog.isEmpty() && !catalog.equals(cluster)) {
                return new LocalResultSet(null, new LocalColumn[0], new Object[0][0]);
            }
            LocalColumn[] columns = new LocalColumn[]{
                new LocalColumn("TABLE_CATALOG", Types.VARCHAR),
                new LocalColumn("TABLE_SCHEM", Types.VARCHAR),
                new LocalColumn("STRATEGY_CLASS", Types.VARCHAR),
                new LocalColumn("STRATEGY_OPTIONS", Types.OTHER),
                new LocalColumn("REPLICATION_FACTOR", Types.INTEGER),
            };
            List<KsDef> ksList = new ArrayList<KsDef>();
            for (KsDef ks : readKeyspaces()) {
                if (schemaPattern != null && !schemaPattern.isEmpty() && !CassandraUtils.matchesPattern(ks.getName(), schemaPattern)) {
                    continue;
                }
                ksList.add(ks);
            }
            Object[][] rows = new Object[ksList.size()][columns.length];
            for (int i = 0; i < ksList.size(); i++) {
                KsDef ksDef = ksList.get(i);
                rows[i][0] = cluster;
                rows[i][1] = ksDef.getName();
                rows[i][2] = ksDef.getStrategy_class();
                rows[i][3] = ksDef.getStrategy_options();
                rows[i][4] = ksDef.getReplication_factor();
            }
            return new LocalResultSet(null, columns, rows);
        } catch (Exception e) {
            throw new SQLException(e);
        }
    }

    public String getSearchStringEscape() throws SQLException
    {
        return "\\";
    }

    public String getStringFunctions() throws SQLException
    {
        return "";
    }

    public ResultSet getSuperTables(String arg0, String arg1, String arg2) throws SQLException
    {
        return new LocalResultSet();
    }

    public ResultSet getSuperTypes(String arg0, String arg1, String arg2) throws SQLException
    {
        return new LocalResultSet();
    }

    public String getSystemFunctions() throws SQLException
    {
        return "";
    }

    public ResultSet getTablePrivileges(String arg0, String arg1, String arg2) throws SQLException
    {
        return new LocalResultSet();
    }

    public ResultSet getTableTypes() throws SQLException
    {
        LocalColumn[] columns = new LocalColumn[]{
            new LocalColumn("TABLE_TYPE", Types.VARCHAR),
        };
        Object[][] rows = {new Object[]{CassandraConstants.CF_TABLE_TYPE}};
        return new LocalResultSet(null, columns, rows);
    }

    public ResultSet getTables(String catalog, String schemaPattern, String tableNamePattern, String types[]) throws SQLException
    {
        try {
            String cluster = showCluster ? connection.getClient().describe_cluster_name() : null;
            if (catalog != null && !catalog.isEmpty() && !catalog.equals(cluster)) {
                return new LocalResultSet(null, new LocalColumn[0], new Object[0][0]);
            }
            List<KsDef> keyspaces = readKeyspaces();
            List<CfDef> columnFamilies = new ArrayList<CfDef>();
            for (KsDef ks : keyspaces) {
                if (schemaPattern != null && !schemaPattern.isEmpty() && !CassandraUtils.matchesPattern(ks.getName(), schemaPattern)) {
                    continue;
                }
                for (CfDef cf : ks.getCf_defs()) {
                    if (tableNamePattern != null && !tableNamePattern.isEmpty() && !CassandraUtils.matchesPattern(cf.getName(), tableNamePattern)) {
                        continue;
                    }
                    columnFamilies.add(cf);
                }
            }
            LocalColumn[] columns = new LocalColumn[]{
                new LocalColumn("TABLE_CATALOG", Types.VARCHAR),
                new LocalColumn("TABLE_SCHEM", Types.VARCHAR),
                new LocalColumn("TABLE_NAME", Types.VARCHAR),
                new LocalColumn("TABLE_TYPE", Types.VARCHAR),
                new LocalColumn("REMARKS", Types.VARCHAR),
                new LocalColumn("TYPE_CAT", Types.VARCHAR),
                new LocalColumn("TYPE_SCHEM", Types.VARCHAR),
                new LocalColumn("TYPE_NAME", Types.VARCHAR),
                new LocalColumn("SELF_REFERENCING_COL_NAME", Types.VARCHAR),
                new LocalColumn("REF_GENERATION", Types.VARCHAR),
                new LocalColumn("CF_CACHING", Types.VARCHAR),
                new LocalColumn("CF_KEY_ALIAS", Types.VARCHAR),
                new LocalColumn("CF_KEY_VALIDATION_CLASS", Types.VARCHAR),
                new LocalColumn("CF_DEFAULT_VALIDATION_CLASS", Types.VARCHAR),
                new LocalColumn("CF_COMPACTION_STRATEGY", Types.VARCHAR),
                new LocalColumn("CF_COMPACTION_STRATEGY_OPTIONS", Types.OTHER),
                new LocalColumn("CF_MAX_COMPACTION_THRESHOLD", Types.INTEGER),
                new LocalColumn("CF_MIN_COMPACTION_THRESHOLD", Types.INTEGER),
                new LocalColumn("CF_COMPARATOR_TYPE", Types.VARCHAR),
                new LocalColumn("CF_SUBCOMPARATOR_TYPE", Types.VARCHAR),
                new LocalColumn("CF_ID", Types.INTEGER),
                new LocalColumn("CF_COMPRESSION_OPTIONS", Types.OTHER),
                new LocalColumn("CF_GC_GRACE_SECONDS", Types.INTEGER),
                new LocalColumn("CF_BLOOM_FILTER_FP_CHANCE", Types.DOUBLE),
                new LocalColumn("CF_DCLOCAL_READ_REPAIR_CHANCE", Types.DOUBLE),
                new LocalColumn("CF_READ_REPAIR_CHANCE", Types.DOUBLE),
            };
            Object[][] rows = new Object[columnFamilies.size()][columns.length];
            for (int i = 0; i < columnFamilies.size(); i++) {
                CfDef cf = columnFamilies.get(i);

                String keyAlias;
                if (cf.getKey_alias() != null) {
                    keyAlias = CassandraUtils.string(cf.getKey_alias());
                } else {
                    keyAlias = CassandraConstants.DEFAULT_KEY_ALIAS;
                }

                rows[i][0] = cluster;
                rows[i][1] = cf.getKeyspace();
                rows[i][2] = cf.getName();
                rows[i][3] = cf.getColumn_type();
                rows[i][4] = cf.getComment();
                rows[i][5] = null;
                rows[i][6] = null;
                rows[i][7] = null;
                rows[i][8] = null;
                rows[i][9] = null;
                try {
                    rows[i][10] = cf.getCaching();
                } catch (Throwable e) {
                    // May be unsupported by 0.x
                }
                rows[i][11] = keyAlias;
                try {
                    rows[i][12] = cf.getKey_validation_class();
                } catch (Throwable e) {
                    // May be unsupported
                }
                try {
                    rows[i][13] = cf.getDefault_validation_class();
                } catch (Throwable e) {
                    // May be unsupported
                }
                try {
                    rows[i][14] = cf.getCompaction_strategy();
                    rows[i][15] = cf.getCompaction_strategy_options();
                    rows[i][16] = cf.getMax_compaction_threshold();
                    rows[i][17] = cf.getMin_compaction_threshold();
                } catch (Throwable e) {
                    // May be unsupported
                }
                try {
                    rows[i][18] = cf.getComparator_type();
                    rows[i][19] = cf.getSubcomparator_type();
                } catch (Throwable e) {
                    // May be unsupported
                }
                try {
                    rows[i][20] = cf.getId();
                } catch (Throwable e) {
                    // May be unsupported
                }
                try {
                    rows[i][21] = cf.getCompression_options();
                } catch (Throwable e) {
                    // May be unsupported
                }
                try {
                    rows[i][22] = cf.getGc_grace_seconds();
                    rows[i][23] = cf.getBloom_filter_fp_chance();
                    rows[i][24] = cf.getDclocal_read_repair_chance();
                    rows[i][25] = cf.getRead_repair_chance();
                } catch (Throwable e) {
                    // May be unsupported
                }
            }
            return new LocalResultSet(null, columns, rows);
        } catch (Exception e) {
            throw new SQLException(e);
        }
    }

    public String getTimeDateFunctions() throws SQLException
    {
        return "";
    }

    public ResultSet getTypeInfo() throws SQLException
    {
        return new LocalResultSet();
    }

    public ResultSet getUDTs(String arg0, String arg1, String arg2, int[] arg3) throws SQLException
    {
        return new LocalResultSet();
    }

    public String getURL() throws SQLException
    {
        return connection.getUrl();
    }

    public String getUserName() throws SQLException
    {
        return (connection.getUsername() == null) ? "" : connection.getUsername();
    }

    public ResultSet getVersionColumns(String arg0, String arg1, String arg2) throws SQLException
    {
        return new LocalResultSet();
    }

    public boolean insertsAreDetected(int arg0) throws SQLException
    {
        return false;
    }

    public boolean isCatalogAtStart() throws SQLException
    {
        return false;
    }

    public boolean isReadOnly() throws SQLException
    {
        return false;
    }

    public boolean locatorsUpdateCopy() throws SQLException
    {
        return false;
    }

    public boolean nullPlusNonNullIsNull() throws SQLException
    {
        return false;
    }

    public boolean nullsAreSortedAtEnd() throws SQLException
    {
        return false;
    }

    public boolean nullsAreSortedAtStart() throws SQLException
    {
        return true;
    }

    public boolean nullsAreSortedHigh() throws SQLException
    {
        return true;
    }

    public boolean nullsAreSortedLow() throws SQLException
    {

        return false;
    }

    public boolean othersDeletesAreVisible(int arg0) throws SQLException
    {
        return false;
    }

    public boolean othersInsertsAreVisible(int arg0) throws SQLException
    {
        return false;
    }

    public boolean othersUpdatesAreVisible(int arg0) throws SQLException
    {
        return false;
    }

    public boolean ownDeletesAreVisible(int arg0) throws SQLException
    {
        return false;
    }

    public boolean ownInsertsAreVisible(int arg0) throws SQLException
    {
        return false;
    }

    public boolean ownUpdatesAreVisible(int arg0) throws SQLException
    {
        return false;
    }

    public boolean storesLowerCaseIdentifiers() throws SQLException
    {
        return false;
    }

    public boolean storesLowerCaseQuotedIdentifiers() throws SQLException
    {
        return false;
    }

    public boolean storesMixedCaseIdentifiers() throws SQLException
    {
        return true;
    }

    public boolean storesMixedCaseQuotedIdentifiers() throws SQLException
    {
        return true;
    }

    public boolean storesUpperCaseIdentifiers() throws SQLException
    {
        return false;
    }

    public boolean storesUpperCaseQuotedIdentifiers() throws SQLException
    {
        return false;
    }

    public boolean supportsANSI92EntryLevelSQL() throws SQLException
    {
        return false;
    }

    public boolean supportsANSI92FullSQL() throws SQLException
    {
        return false;
    }

    public boolean supportsANSI92IntermediateSQL() throws SQLException
    {
        return false;
    }

    public boolean supportsAlterTableWithAddColumn() throws SQLException
    {
        return true;
    }

    public boolean supportsAlterTableWithDropColumn() throws SQLException
    {
        return true;
    }

    public boolean supportsBatchUpdates() throws SQLException
    {
        return false;
    }

    public boolean supportsCatalogsInDataManipulation() throws SQLException
    {
        return false;
    }

    public boolean supportsCatalogsInIndexDefinitions() throws SQLException
    {
        return false;
    }

    public boolean supportsCatalogsInPrivilegeDefinitions() throws SQLException
    {
        return false;
    }

    public boolean supportsCatalogsInProcedureCalls() throws SQLException
    {
        return false;
    }

    public boolean supportsCatalogsInTableDefinitions() throws SQLException
    {
        return false;
    }

    public boolean supportsColumnAliasing() throws SQLException
    {
        return false;
    }

    public boolean supportsConvert() throws SQLException
    {
        return false;
    }

    public boolean supportsConvert(int arg0, int arg1) throws SQLException
    {
        return false;
    }

    public boolean supportsCoreSQLGrammar() throws SQLException
    {
        return false;
    }

    public boolean supportsCorrelatedSubqueries() throws SQLException
    {
        return false;
    }

    public boolean supportsDataDefinitionAndDataManipulationTransactions() throws SQLException
    {
        return false;
    }

    public boolean supportsDataManipulationTransactionsOnly() throws SQLException
    {
        return false;
    }

    public boolean supportsDifferentTableCorrelationNames() throws SQLException
    {
        return false;
    }

    public boolean supportsExpressionsInOrderBy() throws SQLException
    {
        return false;
    }

    public boolean supportsExtendedSQLGrammar() throws SQLException
    {
        return false;
    }

    public boolean supportsFullOuterJoins() throws SQLException
    {
        return false;
    }

    public boolean supportsGetGeneratedKeys() throws SQLException
    {
        return false;
    }

    public boolean supportsGroupBy() throws SQLException
    {
        return false;
    }

    public boolean supportsGroupByBeyondSelect() throws SQLException
    {
        return false;
    }

    public boolean supportsGroupByUnrelated() throws SQLException
    {
        return false;
    }

    public boolean supportsIntegrityEnhancementFacility() throws SQLException
    {
        return false;
    }

    public boolean supportsLikeEscapeClause() throws SQLException
    {

        return false;
    }

    public boolean supportsLimitedOuterJoins() throws SQLException
    {
        return false;
    }

    public boolean supportsMinimumSQLGrammar() throws SQLException
    {
        return false;
    }

    public boolean supportsMixedCaseIdentifiers() throws SQLException
    {
        return true;
    }

    public boolean supportsMixedCaseQuotedIdentifiers() throws SQLException
    {
        return true;
    }

    public boolean supportsMultipleOpenResults() throws SQLException
    {
        return false;
    }

    public boolean supportsMultipleResultSets() throws SQLException
    {
        return false;
    }

    public boolean supportsMultipleTransactions() throws SQLException
    {
        return false;
    }

    public boolean supportsNamedParameters() throws SQLException
    {
        return false;
    }

    public boolean supportsNonNullableColumns() throws SQLException
    {

        return false;
    }

    public boolean supportsOpenCursorsAcrossCommit() throws SQLException
    {
        return false;
    }

    public boolean supportsOpenCursorsAcrossRollback() throws SQLException
    {
        return false;
    }

    public boolean supportsOpenStatementsAcrossCommit() throws SQLException
    {
        return false;
    }

    public boolean supportsOpenStatementsAcrossRollback() throws SQLException
    {
        return false;
    }

    public boolean supportsOrderByUnrelated() throws SQLException
    {
        return false;
    }

    public boolean supportsOuterJoins() throws SQLException
    {
        return false;
    }

    public boolean supportsPositionedDelete() throws SQLException
    {
        return false;
    }

    public boolean supportsPositionedUpdate() throws SQLException
    {
        return false;
    }

    public boolean supportsResultSetConcurrency(int arg0, int arg1) throws SQLException
    {
        return false;
    }

    public boolean supportsResultSetHoldability(int holdability) throws SQLException
    {
        return ResultSet.HOLD_CURSORS_OVER_COMMIT == holdability;
    }

    public boolean supportsResultSetType(int type) throws SQLException
    {

        return ResultSet.TYPE_FORWARD_ONLY == type;
    }

    public boolean supportsSavepoints() throws SQLException
    {
        return false;
    }

    public boolean supportsSchemasInDataManipulation() throws SQLException
    {
        return connection.isVersion11();
    }

    public boolean supportsSchemasInIndexDefinitions() throws SQLException
    {
        return false;
    }

    public boolean supportsSchemasInPrivilegeDefinitions() throws SQLException
    {
        return false;
    }

    public boolean supportsSchemasInProcedureCalls() throws SQLException
    {
        return false;
    }

    public boolean supportsSchemasInTableDefinitions() throws SQLException
    {
        return true;
    }

    public boolean supportsSelectForUpdate() throws SQLException
    {
        return false;
    }

    public boolean supportsStatementPooling() throws SQLException
    {
        return false;
    }

    public boolean supportsStoredFunctionsUsingCallSyntax() throws SQLException
    {
        return false;
    }

    public boolean supportsStoredProcedures() throws SQLException
    {
        return false;
    }

    public boolean supportsSubqueriesInComparisons() throws SQLException
    {
        return false;
    }

    public boolean supportsSubqueriesInExists() throws SQLException
    {
        return false;
    }

    public boolean supportsSubqueriesInIns() throws SQLException
    {
        return false;
    }

    public boolean supportsSubqueriesInQuantifieds() throws SQLException
    {
        return false;
    }

    public boolean supportsTableCorrelationNames() throws SQLException
    {
        return false;
    }

    public boolean supportsTransactionIsolationLevel(int level) throws SQLException
    {

        return Connection.TRANSACTION_NONE == level;
    }

    public boolean supportsTransactions() throws SQLException
    {
        return false;
    }

    public boolean supportsUnion() throws SQLException
    {
        return false;
    }

    public boolean supportsUnionAll() throws SQLException
    {
        return false;
    }

    public boolean updatesAreDetected(int arg0) throws SQLException
    {
        return false;
    }

    public boolean usesLocalFilePerTable() throws SQLException
    {
        return false;
    }

    public boolean usesLocalFiles() throws SQLException
    {
        return false;
    }

    public boolean generatedKeyAlwaysReturned() throws SQLException
    {
        throw new SQLFeatureNotSupportedException();
    }

    public ResultSet getPseudoColumns(String catalog, String schemaPattern, String tableNamePattern, String columnNamePattern) throws SQLException
    {
        throw new SQLFeatureNotSupportedException();
    }

    private List<TableColumnDef> getColumnDefs(String catalog, String schemaPattern, String tableNamePattern) throws TException, InvalidRequestException, SQLException, CharacterCodingException
    {
        String cluster = showCluster ? connection.getClient().describe_cluster_name() : null;
        if (catalog != null && !catalog.isEmpty() && !catalog.equals(cluster)) {
            return Collections.emptyList();
        }
        List<KsDef> keyspaces = readKeyspaces();
        List<TableColumnDef> cfColumns = new ArrayList<TableColumnDef>();
        for (KsDef ks : keyspaces) {
            if (schemaPattern != null && !schemaPattern.isEmpty() && !CassandraUtils.matchesPattern(ks.getName(), schemaPattern)) {
                continue;
            }
            for (CfDef cf : ks.getCf_defs()) {
                if (tableNamePattern != null && !tableNamePattern.isEmpty() && !CassandraUtils.matchesPattern(cf.getName(), tableNamePattern)) {
                    continue;
                }
                int position = 1;
                cfColumns.add(new TableColumnDef(
                    ks,
                    cf,
                    CassandraUtils.getKeyAlias(cf),
                    cf.getKey_validation_class(),
                    position,
                    null));
                for (ColumnDef col : cf.getColumn_metadata()) {
                    position++;
                    if (!col.isSetName()) {
                        continue;
                    }
                    String columnName = CassandraUtils.string(col.getName());
                    cfColumns.add(new TableColumnDef(ks, cf, columnName, col.getValidation_class(), position, col));
                }
            }
        }
        return cfColumns;
    }


}
