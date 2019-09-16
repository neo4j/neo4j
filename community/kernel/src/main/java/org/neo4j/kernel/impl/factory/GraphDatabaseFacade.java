/*
 * Copyright (c) 2002-2019 "Neo4j,"
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
import java.util.function.Function;

import org.neo4j.common.DependencyResolver;
import org.neo4j.configuration.Config;
import org.neo4j.graphdb.QueryExecutionException;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.ResultConsumer;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.TransactionTerminatedException;
import org.neo4j.graphdb.schema.Schema;
import org.neo4j.internal.kernel.api.connectioninfo.ClientConnectionInfo;
import org.neo4j.internal.kernel.api.exceptions.TransactionFailureException;
import org.neo4j.internal.kernel.api.security.LoginContext;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.kernel.GraphDatabaseQueryService;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.kernel.availability.DatabaseAvailabilityGuard;
import org.neo4j.kernel.availability.UnavailableException;
import org.neo4j.kernel.database.Database;
import org.neo4j.kernel.database.DatabaseId;
import org.neo4j.kernel.impl.core.EmbeddedProxySPI;
import org.neo4j.kernel.impl.core.NodeProxy;
import org.neo4j.kernel.impl.core.RelationshipProxy;
import org.neo4j.kernel.impl.core.ThreadToStatementContextBridge;
import org.neo4j.kernel.impl.coreapi.InternalTransaction;
import org.neo4j.kernel.impl.coreapi.TopLevelTransaction;
import org.neo4j.kernel.impl.coreapi.schema.SchemaImpl;
import org.neo4j.kernel.impl.query.Neo4jTransactionalContextFactory;
import org.neo4j.kernel.impl.query.QueryExecutionKernelException;
import org.neo4j.kernel.impl.query.TransactionalContext;
import org.neo4j.kernel.impl.query.TransactionalContextFactory;
import org.neo4j.kernel.impl.util.ValueUtils;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.storageengine.api.StoreId;
import org.neo4j.token.TokenHolders;
import org.neo4j.token.api.TokenNotFoundException;
import org.neo4j.values.virtual.MapValue;

import static java.util.Collections.emptyMap;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.neo4j.configuration.GraphDatabaseSettings.transaction_timeout;
import static org.neo4j.graphdb.ResultConsumer.EMPTY_CONSUMER;
import static org.neo4j.internal.kernel.api.Transaction.Type.implicit;
import static org.neo4j.internal.kernel.api.connectioninfo.ClientConnectionInfo.EMBEDDED_CONNECTION;
import static org.neo4j.internal.kernel.api.security.LoginContext.AUTH_DISABLED;

/**
 * Default implementation of the GraphDatabaseService interface.
 */
public class GraphDatabaseFacade implements GraphDatabaseAPI, EmbeddedProxySPI
{
    private final Schema schema;
    private final Database database;
    private final ThreadToStatementContextBridge statementContext;
    private final TransactionalContextFactory contextFactory;
    private final Config config;
    private final TokenHolders tokenHolders;
    private final DatabaseAvailabilityGuard availabilityGuard;
    private final DatabaseInfo databaseInfo;
    private Function<LoginContext, LoginContext> loginContextTransformer = Function.identity();

    public GraphDatabaseFacade( GraphDatabaseFacade facade, Function<LoginContext,LoginContext> loginContextTransformer )
    {
        this( facade.database, facade.statementContext, facade.config, facade.databaseInfo, facade.availabilityGuard );
        this.loginContextTransformer = requireNonNull( loginContextTransformer );
    }

    public GraphDatabaseFacade( Database database, ThreadToStatementContextBridge txBridge, Config config, DatabaseInfo databaseInfo,
            DatabaseAvailabilityGuard availabilityGuard )
    {
        this.database = requireNonNull( database );
        this.config = requireNonNull( config );
        this.statementContext = requireNonNull( txBridge );
        this.availabilityGuard = requireNonNull( availabilityGuard );
        this.databaseInfo = requireNonNull( databaseInfo );
        this.schema = new SchemaImpl( () -> txBridge.getKernelTransactionBoundToThisThread( true, databaseId() ) );
        this.tokenHolders = database.getTokenHolders();
        this.contextFactory = Neo4jTransactionalContextFactory.create( this,
                () -> getDependencyResolver().resolveDependency( GraphDatabaseQueryService.class ),
                new FacadeKernelTransactionFactory( config, this ),
                txBridge );
    }

    @Override
    public Schema schema()
    {
        assertTransactionOpen();
        return schema;
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
        return beginTransaction( KernelTransaction.Type.explicit, AUTH_DISABLED );
    }

    @Override
    public Transaction beginTx( long timeout, TimeUnit unit )
    {
        return beginTransaction( KernelTransaction.Type.explicit, AUTH_DISABLED, EMBEDDED_CONNECTION, timeout, unit );
    }

    @Override
    public InternalTransaction beginTransaction( KernelTransaction.Type type, LoginContext loginContext )
    {
        return beginTransaction( type, loginContext, EMBEDDED_CONNECTION );
    }

    @Override
    public InternalTransaction beginTransaction( KernelTransaction.Type type, LoginContext loginContext, ClientConnectionInfo clientInfo )
    {
        return beginTransactionInternal( type, loginContext, clientInfo, config.get( transaction_timeout ).toMillis() );
    }

    @Override
    public InternalTransaction beginTransaction( KernelTransaction.Type type, LoginContext loginContext, ClientConnectionInfo clientInfo, long timeout,
            TimeUnit unit )
    {
        return beginTransactionInternal( type, loginContext, clientInfo, unit.toMillis( timeout ) );
    }

    @Override
    public void executeTransactionally( String query ) throws QueryExecutionException
    {
        executeTransactionally( query, emptyMap(), EMPTY_CONSUMER );
    }

    @Override
    public void executeTransactionally( String query, Map<String,Object> parameters, ResultConsumer resultConsumer ) throws QueryExecutionException
    {
        executeTransactionally( query, parameters, resultConsumer, config.get( transaction_timeout ) );
    }

    @Override
    public void executeTransactionally( String query, Map<String,Object> parameters, ResultConsumer resultConsumer, Duration timeout )
            throws QueryExecutionException
    {
        try ( var internalTransaction = beginTransaction( implicit, AUTH_DISABLED, EMBEDDED_CONNECTION, timeout.toMillis(), MILLISECONDS ) )
        {
            try ( var result = execute( internalTransaction, query, ValueUtils.asParameterMapValue( parameters ) ) )
            {
                resultConsumer.accept( result );
            }
            internalTransaction.commit();
        }
    }

    public Result execute( InternalTransaction transaction, String query, MapValue parameters )
            throws QueryExecutionException
    {
        TransactionalContext context = contextFactory.newContext( transaction, query, parameters );
        try
        {
            availabilityGuard.assertDatabaseAvailable();
            return database.getExecutionEngine().executeQuery( query, parameters, context, false );
        }
        catch ( UnavailableException ue )
        {
            throw new org.neo4j.graphdb.TransactionFailureException( ue.getMessage(), ue );
        }
        catch ( QueryExecutionKernelException e )
        {
            throw e.asUserException();
        }
    }

    private InternalTransaction beginTransactionInternal( KernelTransaction.Type type, LoginContext loginContext, ClientConnectionInfo connectionInfo,
            long timeoutMillis )
    {
        if ( statementContext.hasTransaction() )
        {
            throw new org.neo4j.graphdb.TransactionFailureException( "Fail to start new transaction. Already have transaction in the context." );
        }
        final KernelTransaction kernelTransaction = beginKernelTransaction( type, loginContext, connectionInfo, timeoutMillis );
        return new TopLevelTransaction( this, kernelTransaction );
    }

    @Override
    public DatabaseId databaseId()
    {
        return database.getDatabaseId();
    }

    KernelTransaction beginKernelTransaction( KernelTransaction.Type type, LoginContext loginContext, ClientConnectionInfo connectionInfo,
            long timeout )
    {
        try
        {
            availabilityGuard.assertDatabaseAvailable();
            KernelTransaction kernelTx = database.getKernel().beginTransaction( type, loginContextTransformer.apply( loginContext ), connectionInfo, timeout );
            kernelTx.registerCloseListener( txId -> statementContext.unbindTransactionFromCurrentThread() );
            statementContext.bindTransactionToCurrentThread( kernelTx );
            return kernelTx;
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
    public StoreId storeId()
    {
        return database.getStoreId();
    }

    @Override
    public DatabaseLayout databaseLayout()
    {
        return database.getDatabaseLayout();
    }

    @Override
    public String toString()
    {
        return databaseInfo + " [" + databaseLayout() + "]";
    }

    @Override
    public KernelTransaction kernelTransaction()
    {
        return statementContext.getKernelTransactionBoundToThisThread( true, databaseId() );
    }

    @Override
    public void assertInUnterminatedTransaction()
    {
        statementContext.assertInUnterminatedTransaction();
    }

    @Override
    public RelationshipProxy newRelationshipProxy( long id )
    {
        return new RelationshipProxy( this, id );
    }

    @Override
    public RelationshipProxy newRelationshipProxy( long id, long startNodeId, int typeId, long endNodeId )
    {
        return new RelationshipProxy( this, id, startNodeId, typeId, endNodeId );
    }

    @Override
    public NodeProxy newNodeProxy( long nodeId )
    {
        return new NodeProxy( this, nodeId );
    }

    @Override
    public RelationshipType getRelationshipTypeById( int type )
    {
        try
        {
            String name = tokenHolders.relationshipTypeTokens().getTokenById( type ).name();
            return RelationshipType.withName( name );
        }
        catch ( TokenNotFoundException e )
        {
            throw new IllegalStateException( "Kernel API returned non-existent relationship type: " + type );
        }
    }

    private void assertTransactionOpen()
    {
        KernelTransaction transaction = statementContext.getKernelTransactionBoundToThisThread( true, databaseId() );
        if ( transaction.isTerminated() )
        {
            Status terminationReason = transaction.getReasonIfTerminated().orElse( Status.Transaction.Terminated );
            throw new TransactionTerminatedException( terminationReason );
        }
    }
}
