/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * GNU AFFERO GENERAL PUBLIC LICENSE Version 3
 * (http://www.fsf.org/licensing/licenses/agpl-3.0.html) with the
 * Commons Clause, as found in the associated LICENSE.txt file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * Neo4j object code can be licensed independently from the source
 * under separate terms from the AGPL. Inquiries can be directed to:
 * licensing@neo4j.com
 *
 * More information is also available at:
 * https://neo4j.com/licensing/
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
}
