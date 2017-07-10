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

import org.apache.cassandra.thrift.*;
import org.apache.thrift.TException;

import java.sql.*;

import static org.jkiss.jdbc.cassandra.ErrorMessages.NO_SERVER;
import static org.jkiss.jdbc.cassandra.ErrorMessages.SCHEMA_MISMATCH;

public class CassandraPreparedStatementLegacy extends CassandraPreparedStatement {

    CassandraPreparedStatementLegacy(CassandraConnection con, String cql) throws SQLException
    {
        super(con, cql);
    }

    protected void doExecute() throws SQLException
    {
        try {
            resetResults();
            CqlResult result = connection.executeCQL(cql, maxRows);
            switch (result.getType()) {
                case ROWS:
                    currentResultSet = new CassandraResultSet(this, result, keyspace, columnFamily);
                    break;
                case INT:
                    updateCount = result.getNum();
                    break;
                case VOID:
                    updateCount = 0;
                    break;
            }
        } catch (InvalidRequestException e) {
            throw new SQLSyntaxErrorException(e.getWhy(), e);
        } catch (UnavailableException e) {
            throw new SQLNonTransientConnectionException(NO_SERVER, e);
        } catch (TimedOutException e) {
            throw new SQLTransientConnectionException(e.getMessage());
        } catch (SchemaDisagreementException e) {
            throw new SQLRecoverableException(SCHEMA_MISMATCH, e);
        } catch (TException e) {
            throw new SQLNonTransientConnectionException(e.getMessage(), e);
        }
    }

}
