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
package org.neo4j.router.transaction;

import static java.util.Collections.emptyMap;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import org.neo4j.bolt.protocol.common.message.AccessMode;
import org.neo4j.bolt.protocol.common.message.request.connection.RoutingContext;
import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.fabric.transaction.StatementLifecycleTransactionInfo;
import org.neo4j.internal.kernel.api.connectioninfo.ClientConnectionInfo;
import org.neo4j.internal.kernel.api.security.LoginContext;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.database.NormalizedDatabaseName;

/**
 * Container for all the information given by the bolt server for a given transaction
 */
public record TransactionInfo(
        NormalizedDatabaseName sessionDatabaseName,
        KernelTransaction.Type type,
        LoginContext loginContext,
        ClientConnectionInfo clientInfo,
        List<String> bookmarks,
        Duration txTimeout,
        AccessMode accessMode,
        Map<String, Object> txMetadata,
        RoutingContext routingContext,
        org.neo4j.kernel.impl.query.QueryExecutionConfiguration queryExecutionConfiguration) {

    public TransactionInfo withDefaults(Config config) {
        return new TransactionInfo(
                sessionDatabaseName,
                type,
                loginContext,
                clientInfo,
                bookmarks,
                txTimeout != null ? txTimeout : config.get(GraphDatabaseSettings.transaction_timeout),
                accessMode,
                txMetadata != null ? txMetadata : emptyMap(),
                routingContext,
                queryExecutionConfiguration);
    }

    public StatementLifecycleTransactionInfo statementLifecycleTransactionInfo() {
        return new StatementLifecycleTransactionInfo(loginContext, clientInfo, txMetadata);
    }
}
