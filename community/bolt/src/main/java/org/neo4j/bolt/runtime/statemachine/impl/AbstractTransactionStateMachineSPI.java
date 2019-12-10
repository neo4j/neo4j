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
package org.neo4j.bolt.runtime.statemachine.impl;

import java.time.Clock;
import java.time.Duration;
import java.util.List;
import java.util.Map;

import org.neo4j.bolt.BoltChannel;
import org.neo4j.bolt.dbapi.BoltGraphDatabaseServiceSPI;
import org.neo4j.bolt.dbapi.BoltQueryExecution;
import org.neo4j.bolt.dbapi.BoltQueryExecutor;
import org.neo4j.bolt.dbapi.BoltTransaction;
import org.neo4j.bolt.runtime.AccessMode;
import org.neo4j.bolt.runtime.BoltResult;
import org.neo4j.bolt.runtime.BoltResultHandle;
import org.neo4j.bolt.runtime.Bookmark;
import org.neo4j.bolt.runtime.statemachine.StatementProcessorReleaseManager;
import org.neo4j.bolt.runtime.statemachine.TransactionStateMachineSPI;
import org.neo4j.bolt.v3.runtime.bookmarking.BookmarkWithPrefix;
import org.neo4j.exceptions.KernelException;
import org.neo4j.internal.kernel.api.security.LoginContext;
import org.neo4j.kernel.impl.query.QueryExecution;
import org.neo4j.kernel.impl.query.QueryExecutionKernelException;
import org.neo4j.time.SystemNanoClock;
import org.neo4j.values.virtual.MapValue;

import static org.neo4j.kernel.api.KernelTransaction.Type.EXPLICIT;
import static org.neo4j.kernel.api.KernelTransaction.Type.IMPLICIT;

public abstract class AbstractTransactionStateMachineSPI implements TransactionStateMachineSPI
{
    private final BoltGraphDatabaseServiceSPI boltGraphDatabaseServiceSPI;
    private final Clock clock;
    private final BoltChannel boltChannel;
    private final StatementProcessorReleaseManager resourceReleaseManager;

    public AbstractTransactionStateMachineSPI( BoltGraphDatabaseServiceSPI boltGraphDatabaseServiceSPI, BoltChannel boltChannel, SystemNanoClock clock,
            StatementProcessorReleaseManager resourceReleaseManger )
    {
        this.boltGraphDatabaseServiceSPI = boltGraphDatabaseServiceSPI;
        this.boltChannel = boltChannel;
        this.clock = clock;
        this.resourceReleaseManager = resourceReleaseManger;
    }

    @Override
    public Bookmark newestBookmark( BoltTransaction tx )
    {
        var bookmarkMetadata = tx.getBookmarkMetadata();
        return bookmarkMetadata.toBookmark( ( txId, dbId ) -> new BookmarkWithPrefix( txId ) );
    }

    @Override
    public BoltTransaction beginTransaction( LoginContext loginContext, List<Bookmark> bookmarks, Duration txTimeout, AccessMode accessMode,
            Map<String,Object> txMetadata )
    {
        return boltGraphDatabaseServiceSPI.beginTransaction( EXPLICIT, loginContext, boltChannel.info(), bookmarks, txTimeout, accessMode, txMetadata );
    }

    @Override
    public BoltTransaction beginPeriodicCommitTransaction( LoginContext loginContext, List<Bookmark> bookmarks, Duration txTimeout, AccessMode accessMode,
            Map<String,Object> txMetadata )
    {
        return boltGraphDatabaseServiceSPI.beginTransaction( IMPLICIT, loginContext, boltChannel.info(), bookmarks, txTimeout, accessMode, txMetadata );
    }

    @Override
    public boolean isPeriodicCommit( String query )
    {
        return boltGraphDatabaseServiceSPI.isPeriodicCommit( query );
    }

    @Override
    public BoltResultHandle executeQuery( BoltQueryExecutor boltQueryExecutor, String statement, MapValue params )
    {
        return newBoltResultHandle( statement, params, boltQueryExecutor );
    }

    @Override
    public boolean supportsNestedStatementsInTransaction()
    {
        return false;
    }

    @Override
    public void transactionClosed()
    {
        resourceReleaseManager.releaseStatementProcessor();
    }

    protected abstract BoltResultHandle newBoltResultHandle( String statement, MapValue params, BoltQueryExecutor boltQueryExecutor );

    public abstract class AbstractBoltResultHandle implements BoltResultHandle
    {
        private final String statement;
        private final MapValue params;
        private final BoltQueryExecutor boltQueryExecutor;
        private BoltQueryExecution boltQueryExecution;

        public AbstractBoltResultHandle( String statement, MapValue params, BoltQueryExecutor boltQueryExecutor )
        {
            this.statement = statement;
            this.params = params;
            this.boltQueryExecutor = boltQueryExecutor;
        }

        @Override
        public BoltResult start() throws KernelException
        {
            try
            {
                BoltAdapterSubscriber subscriber = new BoltAdapterSubscriber();
                boltQueryExecution = boltQueryExecutor.executeQuery( statement, params, true, subscriber );
                QueryExecution result = boltQueryExecution.getQueryExecution();
                subscriber.assertSucceeded();
                return newBoltResult( result, subscriber, clock );
            }
            catch ( KernelException e )
            {
                close( false );
                throw new QueryExecutionKernelException( e );
            }
            catch ( Throwable e )
            {
                close( false );
                throw e;
            }
        }

        protected abstract BoltResult newBoltResult( QueryExecution result,
                BoltAdapterSubscriber subscriber, Clock clock );

        @Override
        public void close( boolean success )
        {
            if ( boltQueryExecution != null )
            {
                boltQueryExecution.close();
            }
        }

        @Override
        public void terminate()
        {
            if ( boltQueryExecution != null )
            {
                boltQueryExecution.terminate();
            }
        }
    }
}
