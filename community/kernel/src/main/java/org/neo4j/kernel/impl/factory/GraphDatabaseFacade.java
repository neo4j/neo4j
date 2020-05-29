/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
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
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.kernel.impl.factory;

import java.time.Duration;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Function;

import org.neo4j.common.DependencyResolver;
import org.neo4j.configuration.Config;
import org.neo4j.graphdb.QueryExecutionException;
import org.neo4j.graphdb.ResultTransformer;
import org.neo4j.graphdb.Transaction;
import org.neo4j.internal.kernel.api.connectioninfo.ClientConnectionInfo;
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
import org.neo4j.kernel.impl.coreapi.InternalTransaction;
import org.neo4j.kernel.impl.coreapi.TransactionExceptionMapper;
import org.neo4j.kernel.impl.coreapi.TransactionImpl;
import org.neo4j.kernel.impl.query.Neo4jTransactionalContextFactory;
import org.neo4j.kernel.impl.query.TransactionalContextFactory;
import org.neo4j.kernel.internal.GraphDatabaseAPI;

import static java.util.Collections.emptyMap;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.neo4j.configuration.GraphDatabaseSettings.transaction_timeout;
import static org.neo4j.graphdb.ResultTransformer.EMPTY_TRANSFORMER;
import static org.neo4j.internal.kernel.api.connectioninfo.ClientConnectionInfo.EMBEDDED_CONNECTION;
import static org.neo4j.internal.kernel.api.security.LoginContext.AUTH_DISABLED;
import static org.neo4j.kernel.impl.coreapi.DefaultTransactionExceptionMapper.INSTANCE;

/**
 * Default implementation of the GraphDatabaseService interface.
 */
public class GraphDatabaseFacade implements GraphDatabaseAPI
{
    private final Database database;
    protected final TransactionalContextFactory contextFactory;
    private final Config config;
    private final DatabaseAvailabilityGuard availabilityGuard;
    private final DbmsInfo dbmsInfo;
    private Function<LoginContext, LoginContext> loginContextTransformer = Function.identity();

    public GraphDatabaseFacade( GraphDatabaseFacade facade, Function<LoginContext,LoginContext> loginContextTransformer )
    {
        this( facade.database, facade.config, facade.dbmsInfo, facade.availabilityGuard );
        this.loginContextTransformer = requireNonNull( loginContextTransformer );
    }

    public GraphDatabaseFacade( Database database, Config config, DbmsInfo dbmsInfo,
            DatabaseAvailabilityGuard availabilityGuard )
    {
        this.database = requireNonNull( database );
        this.config = requireNonNull( config );
        this.availabilityGuard = requireNonNull( availabilityGuard );
        this.dbmsInfo = requireNonNull( dbmsInfo );
        this.contextFactory = Neo4jTransactionalContextFactory.create( () -> getDependencyResolver().resolveDependency( GraphDatabaseQueryService.class ),
                new FacadeKernelTransactionFactory( config, this ) );
    }

    @Override
    public boolean isAvailable( long timeoutMillis )
    {
        return database.getDatabaseAvailabilityGuard().isAvailable( timeoutMillis );
    }

    @Override
    public Transaction beginTx()
    {
        return beginTransaction();
    }

    protected InternalTransaction beginTransaction()
    {
        return beginTransaction( Type.EXPLICIT, AUTH_DISABLED );
    }

    @Override
    public Transaction beginTx( long timeout, TimeUnit unit )
    {
        return beginTransaction( Type.EXPLICIT, AUTH_DISABLED, EMBEDDED_CONNECTION, timeout, unit );
    }

    @Override
    public InternalTransaction beginTransaction( Type type, LoginContext loginContext )
    {
        return beginTransaction( type, loginContext, EMBEDDED_CONNECTION );
    }

    @Override
    public InternalTransaction beginTransaction( Type type, LoginContext loginContext, ClientConnectionInfo clientInfo )
    {
        return beginTransactionInternal( type, loginContext, clientInfo, config.get( transaction_timeout ).toMillis(), null, INSTANCE );
    }

    @Override
    public InternalTransaction beginTransaction( Type type, LoginContext loginContext, ClientConnectionInfo clientInfo, long timeout,
            TimeUnit unit )
    {
        return beginTransactionInternal( type, loginContext, clientInfo, unit.toMillis( timeout ), null, INSTANCE );
    }

    public InternalTransaction beginTransaction( Type type, LoginContext loginContext, ClientConnectionInfo clientInfo, Consumer<Status> terminationCallback,
            TransactionExceptionMapper transactionExceptionMapper )
    {
        return beginTransactionInternal( type, loginContext, clientInfo, config.get( transaction_timeout ).toMillis(), terminationCallback,
                transactionExceptionMapper );
    }

    @Override
    public void executeTransactionally( String query ) throws QueryExecutionException
    {
        executeTransactionally( query, emptyMap(), EMPTY_TRANSFORMER );
    }

    @Override
    public void executeTransactionally( String query, Map<String,Object> parameters ) throws QueryExecutionException
    {
        executeTransactionally( query, parameters, EMPTY_TRANSFORMER );
    }

    @Override
    public <T> T executeTransactionally( String query, Map<String,Object> parameters, ResultTransformer<T> resultTransformer ) throws QueryExecutionException
    {
        return executeTransactionally( query, parameters, resultTransformer, config.get( transaction_timeout ) );
    }

    @Override
    public <T> T executeTransactionally( String query, Map<String,Object> parameters, ResultTransformer<T> resultTransformer, Duration timeout )
            throws QueryExecutionException
    {
        T transformedResult;
        try ( var internalTransaction = beginTransaction( Type.IMPLICIT, AUTH_DISABLED, EMBEDDED_CONNECTION, timeout.toMillis(), MILLISECONDS ) )
        {
            try ( var result = internalTransaction.execute( query, parameters ) )
            {
                transformedResult = resultTransformer.apply( result );
            }
            internalTransaction.commit();
        }
        return transformedResult;
    }

    protected InternalTransaction beginTransactionInternal( Type type, LoginContext loginContext, ClientConnectionInfo connectionInfo,
            long timeoutMillis, Consumer<Status> terminationCallback, TransactionExceptionMapper transactionExceptionMapper )
    {
        var kernelTransaction = beginKernelTransaction( type, loginContext, connectionInfo, timeoutMillis );
        return new TransactionImpl( database.getTokenHolders(), contextFactory, availabilityGuard, database.getExecutionEngine(), kernelTransaction,
                terminationCallback, transactionExceptionMapper );
    }

    @Override
    public NamedDatabaseId databaseId()
    {
        return database.getNamedDatabaseId();
    }

    @Override
    public DbmsInfo dbmsInfo()
    {
        return dbmsInfo;
    }

    KernelTransaction beginKernelTransaction( Type type, LoginContext loginContext, ClientConnectionInfo connectionInfo, long timeout )
    {
        try
        {
            availabilityGuard.assertDatabaseAvailable();
            return database.getKernel().beginTransaction( type, loginContextTransformer.apply( loginContext ), connectionInfo, timeout );
        }
        catch ( UnavailableException | TransactionFailureException e )
        {
            throw new org.neo4j.graphdb.TransactionFailureException( e.getMessage(), e );
        }
    }

    @Override
    public String databaseName()
    {
        return databaseId().name();
    }

    @Override
    public DependencyResolver getDependencyResolver()
    {
        return database.getDependencyResolver();
    }

    @Override
    public DatabaseLayout databaseLayout()
    {
        return database.getDatabaseLayout();
    }

    @Override
    public String toString()
    {
        return dbmsInfo + " [" + databaseLayout() + "]";
    }
}
