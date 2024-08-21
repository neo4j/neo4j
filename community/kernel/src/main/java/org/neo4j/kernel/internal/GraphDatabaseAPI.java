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
package org.neo4j.kernel.internal;

import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import org.neo4j.common.DependencyResolver;
import org.neo4j.dbms.systemgraph.TopologyGraphDbmsModel.HostedOnMode;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.internal.kernel.api.connectioninfo.ClientConnectionInfo;
import org.neo4j.internal.kernel.api.connectioninfo.RoutingInfo;
import org.neo4j.internal.kernel.api.security.LoginContext;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.kernel.database.NamedDatabaseId;
import org.neo4j.kernel.impl.coreapi.InternalTransaction;
import org.neo4j.kernel.impl.coreapi.TransactionExceptionMapper;
import org.neo4j.kernel.impl.factory.DbmsInfo;

/**
 * This API can be used to get access to services.
 */
public interface GraphDatabaseAPI extends GraphDatabaseService {
    /**
     * Look up database components for direct access.
     * Usage of this method is generally an indication of architectural error.
     */
    DependencyResolver getDependencyResolver();

    /**
     * @return underlying database directory
     */
    DatabaseLayout databaseLayout();

    /**
     * @return underlying database id
     */
    NamedDatabaseId databaseId();

    DbmsInfo dbmsInfo();

    HostedOnMode mode();

    /**
     * Begin internal transaction with specified type and access mode
     * @param type transaction type
     * @param loginContext transaction login context
     * @return internal transaction
     */
    InternalTransaction beginTransaction(KernelTransaction.Type type, LoginContext loginContext);

    /**
     * Begin internal transaction with specified type and access mode
     * @param type transaction type
     * @param loginContext transaction login context
     * @param clientInfo transaction client info
     * @return internal transaction
     */
    InternalTransaction beginTransaction(
            KernelTransaction.Type type, LoginContext loginContext, ClientConnectionInfo clientInfo);

    /**
     * Begin internal transaction with specified type, access mode and timeout
     * @param type transaction type
     * @param loginContext transaction login context
     * @param clientInfo transaction client info
     * @param timeout transaction timeout
     * @param unit time unit of timeout argument
     * @return internal transaction
     */
    InternalTransaction beginTransaction(
            KernelTransaction.Type type,
            LoginContext loginContext,
            ClientConnectionInfo clientInfo,
            long timeout,
            TimeUnit unit);

    /**
     * Begin internal transaction with specified type, access mode and timeout.
     * @param type transaction type
     * @param loginContext transaction login context
     * @param clientInfo transaction client info
     * @param routingInfo routing information provided by the client
     * @param timeout transaction timeout
     * @param unit time unit of timeout argument
     * @param terminationCallback termination callback
     * @param transactionExceptionMapper  transaction exception mapper
     * @return internal transaction
     */
    InternalTransaction beginTransaction(
            KernelTransaction.Type type,
            LoginContext loginContext,
            ClientConnectionInfo clientInfo,
            RoutingInfo routingInfo,
            long timeout,
            TimeUnit unit,
            Consumer<Status> terminationCallback,
            TransactionExceptionMapper transactionExceptionMapper);
}
