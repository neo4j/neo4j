/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */
package org.neo4j.fabric.transaction;

import java.time.Duration;
import java.util.Map;
import org.neo4j.bolt.protocol.common.message.AccessMode;
import org.neo4j.bolt.protocol.common.message.request.connection.RoutingContext;
import org.neo4j.internal.kernel.api.connectioninfo.ClientConnectionInfo;
import org.neo4j.internal.kernel.api.security.LoginContext;
import org.neo4j.kernel.database.DatabaseReference;
import org.neo4j.kernel.impl.query.QueryExecutionConfiguration;

public class FabricTransactionInfo extends StatementLifecycleTransactionInfo {
    private final AccessMode accessMode;
    private final DatabaseReference sessionDatabaseReference;
    private final boolean implicitTransaction;
    private final Duration txTimeout;
    private final RoutingContext routingContext;
    private final QueryExecutionConfiguration queryExecutionConfiguration;

    public FabricTransactionInfo(
            AccessMode accessMode,
            LoginContext loginContext,
            ClientConnectionInfo clientConnectionInfo,
            DatabaseReference sessionDatabaseReference,
            boolean implicitTransaction,
            Duration txTimeout,
            Map<String, Object> txMetadata,
            RoutingContext routingContext,
            QueryExecutionConfiguration queryExecutionConfiguration) {
        super(loginContext, clientConnectionInfo, txMetadata);
        this.accessMode = accessMode;
        this.sessionDatabaseReference = sessionDatabaseReference;
        this.implicitTransaction = implicitTransaction;
        this.txTimeout = txTimeout;
        this.routingContext = routingContext;
        this.queryExecutionConfiguration = queryExecutionConfiguration;
    }

    public AccessMode getAccessMode() {
        return accessMode;
    }

    public DatabaseReference getSessionDatabaseReference() {
        return sessionDatabaseReference;
    }

    public boolean isImplicitTransaction() {
        return implicitTransaction;
    }

    public Duration getTxTimeout() {
        return txTimeout;
    }

    public void setMetaData(Map<String, Object> txMeta) {
        txMetadata = txMeta;
    }

    public RoutingContext getRoutingContext() {
        return routingContext;
    }

    public QueryExecutionConfiguration getQueryExecutionConfiguration() {
        return queryExecutionConfiguration;
    }
}
