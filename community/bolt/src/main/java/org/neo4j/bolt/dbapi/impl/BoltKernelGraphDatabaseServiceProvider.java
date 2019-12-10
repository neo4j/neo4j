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
package org.neo4j.bolt.dbapi.impl;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.neo4j.bolt.dbapi.BoltGraphDatabaseServiceSPI;
import org.neo4j.bolt.dbapi.BoltTransaction;
import org.neo4j.bolt.dbapi.BookmarkMetadata;
import org.neo4j.bolt.runtime.AccessMode;
import org.neo4j.bolt.runtime.Bookmark;
import org.neo4j.bolt.txtracking.TransactionIdTracker;
import org.neo4j.internal.kernel.api.connectioninfo.ClientConnectionInfo;
import org.neo4j.internal.kernel.api.security.LoginContext;
import org.neo4j.kernel.GraphDatabaseQueryService;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.database.Database;
import org.neo4j.kernel.database.NamedDatabaseId;
import org.neo4j.kernel.impl.coreapi.InternalTransaction;
import org.neo4j.kernel.impl.query.Neo4jTransactionalContextFactory;
import org.neo4j.kernel.impl.query.QueryExecutionEngine;
import org.neo4j.kernel.impl.query.TransactionalContextFactory;
import org.neo4j.kernel.internal.GraphDatabaseAPI;

public class BoltKernelGraphDatabaseServiceProvider implements BoltGraphDatabaseServiceSPI
{
    private final TransactionIdTracker transactionIdTracker;
    private final GraphDatabaseAPI databaseAPI;
    private final QueryExecutionEngine queryExecutionEngine;
    private final TransactionalContextFactory transactionalContextFactory;
    private final NamedDatabaseId namedDatabaseId;
    private final Duration perBookmarkTimeout;

    public BoltKernelGraphDatabaseServiceProvider( GraphDatabaseAPI databaseAPI, TransactionIdTracker transactionIdTracker, Duration perBookmarkTimeout )
    {
        this.databaseAPI = databaseAPI;
        this.queryExecutionEngine = resolveDependency( databaseAPI, QueryExecutionEngine.class );
        this.transactionIdTracker = transactionIdTracker;
        this.transactionalContextFactory = newTransactionalContextFactory( databaseAPI );
        this.namedDatabaseId = resolveDependency( databaseAPI, Database.class ).getNamedDatabaseId();
        this.perBookmarkTimeout = perBookmarkTimeout;
    }

    private static <T> T resolveDependency( GraphDatabaseAPI databaseContext, Class<T> clazz )
    {
        return databaseContext.getDependencyResolver().resolveDependency( clazz );
    }

    private static TransactionalContextFactory newTransactionalContextFactory( GraphDatabaseAPI databaseContext )
    {
        GraphDatabaseQueryService queryService = resolveDependency( databaseContext, GraphDatabaseQueryService.class );
        return Neo4jTransactionalContextFactory.create( queryService );
    }

    private void awaitUpToDate( List<Bookmark> bookmarks )
    {
        for ( var bookmark : bookmarks )
        {
            var databaseId = databaseIdFromBookmarkOrCurrent( bookmark );
            transactionIdTracker.awaitUpToDate( databaseId, bookmark.txId(), perBookmarkTimeout );
        }
    }

    private BookmarkMetadata bookmarkWithTxId()
    {
        return new BookmarkMetadata( transactionIdTracker.newestTransactionId( namedDatabaseId ), namedDatabaseId );
    }

    @Override
    public BoltTransaction beginTransaction( KernelTransaction.Type type, LoginContext loginContext, ClientConnectionInfo clientInfo, List<Bookmark> bookmarks,
            Duration txTimeout, AccessMode accessMode, Map<String,Object> txMetadata )
    {
        awaitUpToDate( bookmarks );
        InternalTransaction topLevelInternalTransaction = beginInternalTransaction( type, loginContext, clientInfo, txTimeout, txMetadata );
        KernelTransaction kernelTransaction = topLevelInternalTransaction.kernelTransaction();
        if ( KernelTransaction.Type.IMPLICIT == type )
        {
            return new PeriodicBoltKernelTransaction( queryExecutionEngine, transactionalContextFactory,
                    topLevelInternalTransaction, this::bookmarkWithTxId );
        }
        return new BoltKernelTransaction( queryExecutionEngine, transactionalContextFactory, kernelTransaction, topLevelInternalTransaction,
                this::bookmarkWithTxId );
    }

    @Override
    public boolean isPeriodicCommit( String query )
    {
        return queryExecutionEngine.isPeriodicCommit( query );
    }

    @Override
    public NamedDatabaseId getNamedDatabaseId()
    {
        return namedDatabaseId;
    }

    private InternalTransaction beginInternalTransaction( KernelTransaction.Type type, LoginContext loginContext, ClientConnectionInfo clientInfo,
            Duration txTimeout, Map<String,Object> txMetadata )
    {
        InternalTransaction internalTransaction;
        if ( txTimeout == null )
        {
            internalTransaction = databaseAPI.beginTransaction( type, loginContext, clientInfo );
        }
        else
        {
            internalTransaction = databaseAPI.beginTransaction( type, loginContext, clientInfo, txTimeout.toMillis(), TimeUnit.MILLISECONDS );
        }

        if ( txMetadata != null )
        {
            internalTransaction.setMetaData( txMetadata );
        }

        return internalTransaction;
    }

    private NamedDatabaseId databaseIdFromBookmarkOrCurrent( Bookmark bookmark )
    {
        var specifiedDatabaseId = bookmark.databaseId();
        if ( specifiedDatabaseId == null )
        {
            // bookmark does not contain a database ID so it's an old bookmark and the current database ID should be used
            return namedDatabaseId;
        }
        return specifiedDatabaseId;
    }
}
