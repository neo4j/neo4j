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
package org.neo4j.procedure.impl;

import static java.util.Objects.requireNonNull;

import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Function;
import org.neo4j.common.DependencyResolver;
import org.neo4j.configuration.Config;
import org.neo4j.dbms.systemgraph.TopologyGraphDbmsModel;
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
import org.neo4j.kernel.impl.factory.GraphDatabaseTransactions;
import org.neo4j.kernel.internal.GraphDatabaseAPI;

/**
 * Implementation of {@link org.neo4j.graphdb.GraphDatabaseService} (and {@link GraphDatabaseAPI}) for injection
 * into procedure implementations.
 */
public class ProcedureGraphDatabaseAPI extends GraphDatabaseTransactions implements GraphDatabaseAPI {

    private final GraphDatabaseAPI delegate;
    private final Function<LoginContext, LoginContext> loginContextTransformer;

    public ProcedureGraphDatabaseAPI(
            GraphDatabaseAPI delegate, Function<LoginContext, LoginContext> loginContextTransformer, Config config) {
        super(config);
        this.delegate = requireNonNull(delegate);
        this.loginContextTransformer = requireNonNull(loginContextTransformer);
    }

    @Override
    public boolean isAvailable() {
        return delegate.isAvailable();
    }

    @Override
    public boolean isAvailable(long timeoutMillis) {
        return delegate.isAvailable(timeoutMillis);
    }

    @Override
    public String databaseName() {
        return delegate.databaseName();
    }

    @Override
    public DependencyResolver getDependencyResolver() {
        return delegate.getDependencyResolver();
    }

    @Override
    public DatabaseLayout databaseLayout() {
        return delegate.databaseLayout();
    }

    @Override
    public NamedDatabaseId databaseId() {
        return delegate.databaseId();
    }

    @Override
    public DbmsInfo dbmsInfo() {
        return delegate.dbmsInfo();
    }

    @Override
    public TopologyGraphDbmsModel.HostedOnMode mode() {
        return delegate.mode();
    }

    @Override
    public InternalTransaction beginTransaction(
            KernelTransaction.Type type,
            LoginContext loginContext,
            ClientConnectionInfo clientInfo,
            long timeout,
            TimeUnit unit) {
        return delegate.beginTransaction(type, loginContextTransformer.apply(loginContext), clientInfo, timeout, unit);
    }

    @Override
    public InternalTransaction beginTransaction(
            KernelTransaction.Type type,
            LoginContext loginContext,
            ClientConnectionInfo clientInfo,
            RoutingInfo routingInfo,
            long timeout,
            TimeUnit unit,
            Consumer<Status> terminationCallback,
            TransactionExceptionMapper transactionExceptionMapper) {
        return beginTransaction(type, loginContext, clientInfo, timeout, unit);
    }
}
