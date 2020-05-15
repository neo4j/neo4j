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
package org.neo4j.fabric.executor;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.neo4j.bolt.runtime.AccessMode;
import org.neo4j.cypher.internal.FullyParsedQuery;
import org.neo4j.cypher.internal.javacompat.ExecutionEngine;
import org.neo4j.fabric.FabricDatabaseManager;
import org.neo4j.fabric.bookmark.TransactionBookmarkManager;
import org.neo4j.fabric.config.FabricConfig;
import org.neo4j.fabric.executor.FabricStatementLifecycles.StatementLifecycle;
import org.neo4j.fabric.stream.Record;
import org.neo4j.fabric.stream.StatementResult;
import org.neo4j.fabric.transaction.CompositeTransaction;
import org.neo4j.fabric.transaction.FabricTransactionInfo;
import org.neo4j.fabric.transaction.TransactionMode;
import org.neo4j.graphdb.TransactionFailureException;
import org.neo4j.kernel.GraphDatabaseQueryService;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.kernel.availability.UnavailableException;
import org.neo4j.kernel.impl.coreapi.InternalTransaction;
import org.neo4j.kernel.impl.factory.GraphDatabaseFacade;
import org.neo4j.kernel.impl.query.Neo4jTransactionalContextFactory;
import org.neo4j.values.virtual.MapValue;

public class FabricLocalExecutor
{
    private final FabricConfig config;
    private final FabricDatabaseManager dbms;
    private final FabricDatabaseAccess databaseAccess;

    public FabricLocalExecutor( FabricConfig config, FabricDatabaseManager dbms, FabricDatabaseAccess databaseAccess )
    {
        this.config = config;
        this.dbms = dbms;
        this.databaseAccess = databaseAccess;
    }

    public LocalTransactionContext startTransactionContext( CompositeTransaction compositeTransaction,
            FabricTransactionInfo transactionInfo,
            TransactionBookmarkManager bookmarkManager )
    {
        return new LocalTransactionContext( compositeTransaction, transactionInfo, bookmarkManager );
    }

    public class LocalTransactionContext implements AutoCloseable
    {
        private final Map<Long, KernelTxWrapper> kernelTransactions = new ConcurrentHashMap<>();
        private final Set<InternalTransaction> internalTransactions = ConcurrentHashMap.newKeySet();

        private final CompositeTransaction compositeTransaction;
        private final FabricTransactionInfo transactionInfo;
        private final TransactionBookmarkManager bookmarkManager;

        private LocalTransactionContext( CompositeTransaction compositeTransaction,
                FabricTransactionInfo transactionInfo,
                TransactionBookmarkManager bookmarkManager )
        {
            this.compositeTransaction = compositeTransaction;
            this.transactionInfo = transactionInfo;
            this.bookmarkManager = bookmarkManager;
        }

        public StatementResult run( Location.Local location, TransactionMode transactionMode, StatementLifecycle parentLifecycle,
                                    FullyParsedQuery query, MapValue params, Flux<Record> input, ExecutionOptions executionOptions )
        {
            var kernelTransaction = getOrCreateTx( location, transactionMode );
            return kernelTransaction.run( query, params, input, parentLifecycle, executionOptions );
        }

        @Override
        public void close()
        {

        }

        public Set<InternalTransaction> getInternalTransactions()
        {
            return internalTransactions;
        }

        public FabricKernelTransaction getOrCreateTx( Location.Local location, TransactionMode transactionMode )
        {
            var existingTx = kernelTransactions.get( location.getGraphId() );
            if ( existingTx != null )
            {
                maybeUpgradeToWritingTransaction( existingTx, transactionMode );
                return existingTx.fabricKernelTransaction;
            }

            // it is important to try to get the facade before handling bookmarks
            // Unlike the bookmark logic, this will fail gracefully if the database is not available
            var databaseFacade = getDatabaseFacade( location );

            bookmarkManager.awaitUpToDate( location );
            return kernelTransactions.computeIfAbsent( location.getGraphId(), locationId ->
            {
                switch ( transactionMode )
                {
                case DEFINITELY_WRITE:
                    return compositeTransaction.startWritingTransaction( location, () ->
                    {
                        var tx = beginKernelTx( databaseFacade, AccessMode.WRITE );
                        return new KernelTxWrapper( tx, bookmarkManager, location );
                    } );

                case MAYBE_WRITE:
                    return compositeTransaction.startReadingTransaction( location, () ->
                    {
                        var tx = beginKernelTx( databaseFacade, AccessMode.WRITE );
                        return new KernelTxWrapper( tx, bookmarkManager, location );
                    } );

                case DEFINITELY_READ:
                    return compositeTransaction.startReadingOnlyTransaction( location, () ->
                    {
                        var tx = beginKernelTx( databaseFacade, AccessMode.READ );
                        return new KernelTxWrapper( tx, bookmarkManager, location );
                    } );
                default:
                    throw new IllegalArgumentException( "Unexpected transaction mode: " + transactionMode );
                }
            } ).fabricKernelTransaction;
        }

        private void maybeUpgradeToWritingTransaction( KernelTxWrapper tx, TransactionMode transactionMode )
        {
            if ( transactionMode == TransactionMode.DEFINITELY_WRITE )
            {
                compositeTransaction.upgradeToWritingTransaction( tx );
            }
        }

        private FabricKernelTransaction beginKernelTx( GraphDatabaseFacade databaseFacade, AccessMode accessMode )
        {
            var dependencyResolver = databaseFacade.getDependencyResolver();
            var executionEngine = dependencyResolver.resolveDependency( ExecutionEngine.class );

            var internalTransaction = beginInternalTransaction( databaseFacade, transactionInfo );

            var queryService = dependencyResolver.resolveDependency( GraphDatabaseQueryService.class );
            var transactionalContextFactory = Neo4jTransactionalContextFactory.create( queryService );

            return new FabricKernelTransaction( executionEngine, transactionalContextFactory, internalTransaction, config );
        }

        private GraphDatabaseFacade getDatabaseFacade( Location.Local location )
        {
            try
            {
                return dbms.getDatabase( location.getDatabaseName() );
            }
            catch ( UnavailableException e )
            {
                throw new FabricException( Status.Database.DatabaseUnavailable, e );
            }
        }

        private InternalTransaction beginInternalTransaction( GraphDatabaseFacade databaseFacade, FabricTransactionInfo transactionInfo )
        {
            KernelTransaction.Type kernelTransactionType = getKernelTransactionType( transactionInfo );
            var loginContext = databaseAccess.maybeRestrictLoginContext( transactionInfo.getLoginContext(), databaseFacade.databaseName() );

            var internalTransaction = databaseFacade.beginTransaction( kernelTransactionType, loginContext, transactionInfo.getClientConnectionInfo(),
                    compositeTransaction::childTransactionTerminated, this::transformTerminalOperationError );

            if ( transactionInfo.getTxMetadata() != null )
            {
                internalTransaction.setMetaData( transactionInfo.getTxMetadata() );
            }

            internalTransactions.add( internalTransaction );

            return internalTransaction;
        }

        private KernelTransaction.Type getKernelTransactionType( FabricTransactionInfo fabricTransactionInfo )
        {
            if ( fabricTransactionInfo.isImplicitTransaction() )
            {
                return KernelTransaction.Type.IMPLICIT;
            }

            return KernelTransaction.Type.EXPLICIT;
        }

        private RuntimeException transformTerminalOperationError( Exception e )
        {
            // The main purpose of this is mapping of checked exceptions
            // while preserving status codes
            if ( e instanceof Status.HasStatus )
            {
                if ( e instanceof RuntimeException )
                {
                    return (RuntimeException) e;
                }
                return new FabricException( ((Status.HasStatus) e).status(), e.getMessage(), e );
            }

            // We don't know what operation is being executed,
            // so it is not possible to come up with a reasonable status code here.
            // The error is wrapped into a generic one
            // and a proper status code will be added later.
            throw new TransactionFailureException( "Unable to complete transaction.", e );
        }
    }

    private static class KernelTxWrapper implements SingleDbTransaction
    {

        private final FabricKernelTransaction fabricKernelTransaction;
        private final TransactionBookmarkManager bookmarkManager;
        private final Location.Local location;

        KernelTxWrapper( FabricKernelTransaction fabricKernelTransaction, TransactionBookmarkManager bookmarkManager, Location.Local location )
        {
            this.fabricKernelTransaction = fabricKernelTransaction;
            this.bookmarkManager = bookmarkManager;
            this.location = location;
        }

        @Override
        public Mono<Void> commit()
        {
            return Mono.fromRunnable( this::doCommit );
        }

        @Override
        public Mono<Void> rollback()
        {
            return Mono.fromRunnable( this::doRollback );
        }

        private void doCommit()
        {
            fabricKernelTransaction.commit();
            bookmarkManager.localTransactionCommitted( location );
        }

        private void doRollback()
        {
            fabricKernelTransaction.rollback();
        }

        @Override
        public Mono<Void> terminate( Status reason )
        {
            return Mono.fromRunnable( () -> fabricKernelTransaction.terminate( reason ) );
        }

        @Override
        public Location getLocation()
        {
            return location;
        }
    }
}
