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
package org.neo4j.fabric.bolt;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.neo4j.bolt.dbapi.BoltGraphDatabaseServiceSPI;
import org.neo4j.bolt.dbapi.BoltQueryExecution;
import org.neo4j.bolt.dbapi.BoltTransaction;
import org.neo4j.bolt.dbapi.BookmarkMetadata;
import org.neo4j.bolt.runtime.AccessMode;
import org.neo4j.bolt.runtime.Bookmark;
import org.neo4j.bolt.v41.messaging.RoutingContext;
import org.neo4j.fabric.bookmark.LocalGraphTransactionIdTracker;
import org.neo4j.fabric.bookmark.TransactionBookmarkManagerFactory;
import org.neo4j.fabric.bootstrap.TestOverrides;
import org.neo4j.fabric.config.FabricConfig;
import org.neo4j.fabric.executor.FabricExecutor;
import org.neo4j.fabric.stream.StatementResult;
import org.neo4j.fabric.transaction.FabricTransaction;
import org.neo4j.fabric.transaction.FabricTransactionInfo;
import org.neo4j.fabric.transaction.TransactionManager;
import org.neo4j.internal.kernel.api.connectioninfo.ClientConnectionInfo;
import org.neo4j.internal.kernel.api.security.LoginContext;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.kernel.database.NamedDatabaseId;
import org.neo4j.kernel.impl.query.QuerySubscriber;
import org.neo4j.values.virtual.MapValue;

import static org.neo4j.kernel.api.exceptions.Status.Transaction.Terminated;

public class BoltFabricDatabaseService implements BoltGraphDatabaseServiceSPI
{

    private final FabricExecutor fabricExecutor;
    private final NamedDatabaseId namedDatabaseId;
    private final FabricConfig config;
    private final TransactionManager transactionManager;
    private final LocalGraphTransactionIdTracker transactionIdTracker;
    private final TransactionBookmarkManagerFactory transactionBookmarkManagerFactory;

    public BoltFabricDatabaseService( NamedDatabaseId namedDatabaseId,
                                      FabricExecutor fabricExecutor,
                                      FabricConfig config,
                                      TransactionManager transactionManager,
                                      LocalGraphTransactionIdTracker transactionIdTracker,
                                      TransactionBookmarkManagerFactory transactionBookmarkManagerFactory )
    {
        this.namedDatabaseId = namedDatabaseId;
        this.config = config;
        this.transactionManager = transactionManager;
        this.fabricExecutor = fabricExecutor;
        this.transactionIdTracker = transactionIdTracker;
        this.transactionBookmarkManagerFactory = transactionBookmarkManagerFactory;
    }

    @Override
    public BoltTransaction beginTransaction( KernelTransaction.Type type, LoginContext loginContext, ClientConnectionInfo clientInfo, List<Bookmark> bookmarks,
                                             Duration txTimeout, AccessMode accessMode, Map<String,Object> txMetadata, RoutingContext routingContext )
    {
        if ( txTimeout == null )
        {
            txTimeout = config.getTransactionTimeout();
        }

        FabricTransactionInfo transactionInfo = new FabricTransactionInfo(
                accessMode,
                loginContext,
                clientInfo,
                namedDatabaseId.name(),
                KernelTransaction.Type.IMPLICIT == type,
                txTimeout,
                txMetadata,
                TestOverrides.routingContext( routingContext )
        );

        var transactionBookmarkManager = transactionBookmarkManagerFactory.createTransactionBookmarkManager( transactionIdTracker );
        transactionBookmarkManager.processSubmittedByClient( bookmarks );

        FabricTransaction fabricTransaction = transactionManager.begin( transactionInfo, transactionBookmarkManager );
        return new BoltTransactionImpl( transactionInfo, fabricTransaction );
    }

    @Override
    public boolean isPeriodicCommit( String query )
    {
        return fabricExecutor.isPeriodicCommit( query );
    }

    @Override
    public NamedDatabaseId getNamedDatabaseId()
    {
        return namedDatabaseId;
    }

    public class BoltTransactionImpl implements BoltTransaction
    {
        private final FabricTransaction fabricTransaction;

        BoltTransactionImpl( FabricTransactionInfo transactionInfo, FabricTransaction fabricTransaction )
        {
            this.fabricTransaction = fabricTransaction;
        }

        @Override
        public void commit()
        {
            fabricTransaction.commit();
        }

        @Override
        public void rollback()
        {
            fabricTransaction.rollback();
        }

        @Override
        public void markForTermination( Status reason )
        {
            fabricTransaction.markForTermination( reason );
        }

        @Override
        public void markForTermination()
        {
            fabricTransaction.markForTermination( Terminated );
        }

        @Override
        public Optional<Status> getReasonIfTerminated()
        {
            return fabricTransaction.getReasonIfTerminated();
        }

        @Override
        public BookmarkMetadata getBookmarkMetadata()
        {
            return fabricTransaction.getBookmarkManager().constructFinalBookmark();
        }

        @Override
        public BoltQueryExecution executeQuery( String query, MapValue parameters, boolean prePopulate, QuerySubscriber subscriber )
        {
            StatementResult statementResult = fabricExecutor.run( fabricTransaction, query, parameters );
            final BoltQueryExecutionImpl queryExecution = new BoltQueryExecutionImpl( statementResult, subscriber, config );
            try
            {
                queryExecution.initialize();
            }
            catch ( Exception e )
            {
                QuerySubscriber.safelyOnError( subscriber, e );
            }
            return queryExecution;
        }

        /**
         * This is a hack to be able to get an InternalTransaction for the TestFabricTransaction tx wrapper
         */
        @Deprecated
        public FabricTransaction getFabricTransaction()
        {
            return fabricTransaction;
        }
    }
}
