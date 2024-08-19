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
package org.neo4j.kernel.impl.factory;

import static java.util.Objects.requireNonNull;
import static org.neo4j.kernel.impl.coreapi.DefaultTransactionExceptionMapper.INSTANCE;

import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import org.neo4j.common.DependencyResolver;
import org.neo4j.configuration.Config;
import org.neo4j.dbms.systemgraph.TopologyGraphDbmsModel.HostedOnMode;
import org.neo4j.internal.kernel.api.connectioninfo.ClientConnectionInfo;
import org.neo4j.internal.kernel.api.connectioninfo.RoutingInfo;
import org.neo4j.internal.kernel.api.exceptions.TransactionFailureException;
import org.neo4j.internal.kernel.api.security.LoginContext;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.kernel.GraphDatabaseQueryService;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.api.KernelTransaction.Type;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.kernel.availability.DatabaseAvailabilityGuard;
import org.neo4j.kernel.availability.UnavailableException;
import org.neo4j.kernel.database.Database;
import org.neo4j.kernel.database.NamedDatabaseId;
import org.neo4j.kernel.impl.api.CloseableResourceManager;
import org.neo4j.kernel.impl.coreapi.InternalTransaction;
import org.neo4j.kernel.impl.coreapi.TransactionExceptionMapper;
import org.neo4j.kernel.impl.coreapi.TransactionImpl;
import org.neo4j.kernel.impl.query.Neo4jTransactionalContextFactory;
import org.neo4j.kernel.impl.query.TransactionalContextFactory;
import org.neo4j.kernel.internal.GraphDatabaseAPI;

/**
 * Default implementation of the GraphDatabaseService interface.
 */
public class GraphDatabaseFacade extends GraphDatabaseTransactions implements GraphDatabaseAPI {
    private final Database database;
    protected final TransactionalContextFactory contextFactory;
    private final DatabaseAvailabilityGuard availabilityGuard;
    private final HostedOnMode mode;
    private final DbmsInfo dbmsInfo;

    public GraphDatabaseFacade(
            Database database,
            Config config,
            DbmsInfo dbmsInfo,
            HostedOnMode mode,
            DatabaseAvailabilityGuard availabilityGuard) {
        super(config);
        this.database = requireNonNull(database);
        this.availabilityGuard = requireNonNull(availabilityGuard);
        this.dbmsInfo = requireNonNull(dbmsInfo);
        this.mode = requireNonNull(mode);
        this.contextFactory = Neo4jTransactionalContextFactory.create(
                () -> getDependencyResolver().resolveDependency(GraphDatabaseQueryService.class),
                new FacadeKernelTransactionFactory(config, this));
    }

    @Override
    public boolean isAvailable() {
        return database.getDatabaseAvailabilityGuard().isAvailable();
    }

    @Override
    public boolean isAvailable(long timeoutMillis) {
        return database.getDatabaseAvailabilityGuard().isAvailable(timeoutMillis);
    }

    @Override
    public InternalTransaction beginTransaction(
            Type type, LoginContext loginContext, ClientConnectionInfo clientInfo, long timeout, TimeUnit unit) {
        return beginTransactionInternal(type, loginContext, clientInfo, null, unit.toMillis(timeout), null, INSTANCE);
    }

    @Override
    public InternalTransaction beginTransaction(
            Type type,
            LoginContext loginContext,
            ClientConnectionInfo clientInfo,
            RoutingInfo routingInfo,
            long timeout,
            TimeUnit unit,
            Consumer<Status> terminationCallback,
            TransactionExceptionMapper transactionExceptionMapper) {
        return beginTransactionInternal(
                type,
                loginContext,
                clientInfo,
                routingInfo,
                unit.toMillis(timeout),
                terminationCallback,
                transactionExceptionMapper);
    }

    protected InternalTransaction beginTransactionInternal(
            Type type,
            LoginContext loginContext,
            ClientConnectionInfo connectionInfo,
            RoutingInfo routingInfo,
            long timeoutMillis,
            Consumer<Status> terminationCallback,
            TransactionExceptionMapper transactionExceptionMapper) {
        var kernelTransaction = beginKernelTransaction(type, loginContext, connectionInfo, timeoutMillis);
        return new TransactionImpl(
                database.getTokenHolders(),
                contextFactory,
                availabilityGuard,
                database.getExecutionEngine(),
                kernelTransaction,
                new CloseableResourceManager(),
                terminationCallback,
                transactionExceptionMapper,
                database.getElementIdMapper(),
                routingInfo);
    }

    @Override
    public NamedDatabaseId databaseId() {
        return database.getNamedDatabaseId();
    }

    @Override
    public DbmsInfo dbmsInfo() {
        return dbmsInfo;
    }

    @Override
    public HostedOnMode mode() {
        return mode;
    }

    KernelTransaction beginKernelTransaction(
            Type type, LoginContext loginContext, ClientConnectionInfo connectionInfo, long timeout) {
        try {
            availabilityGuard.assertDatabaseAvailable();
            return database.getKernel().beginTransaction(type, loginContext, connectionInfo, timeout);
        } catch (UnavailableException | TransactionFailureException e) {
            throw new org.neo4j.graphdb.TransactionFailureException(e.getMessage(), e, e.status());
        }
    }

    @Override
    public String databaseName() {
        return databaseId().name();
    }

    @Override
    public DependencyResolver getDependencyResolver() {
        return database.getDependencyResolver();
    }

    @Override
    public DatabaseLayout databaseLayout() {
        return database.getDatabaseLayout();
    }

    @Override
    public String toString() {
        return dbmsInfo + "/" + mode + " [" + databaseLayout() + "]";
    }
}
