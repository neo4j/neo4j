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
package org.neo4j.bolt.v1.runtime;

import java.io.IOException;
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
import org.neo4j.bolt.runtime.TransactionStateMachineSPI;
import org.neo4j.bolt.v1.runtime.bookmarking.BookmarkWithPrefix;
import org.neo4j.exceptions.CypherExecutionException;
import org.neo4j.exceptions.KernelException;
import org.neo4j.graphdb.QueryStatistics;
import org.neo4j.internal.kernel.api.security.LoginContext;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.kernel.impl.query.QueryExecution;
import org.neo4j.kernel.impl.query.QueryExecutionKernelException;
import org.neo4j.kernel.impl.query.QuerySubscriber;
import org.neo4j.time.SystemNanoClock;
import org.neo4j.values.AnyValue;
import org.neo4j.values.virtual.MapValue;

import static org.neo4j.internal.kernel.api.Transaction.Type.explicit;
import static org.neo4j.internal.kernel.api.Transaction.Type.implicit;

public class TransactionStateMachineV1SPI implements TransactionStateMachineSPI
{
    private final BoltGraphDatabaseServiceSPI boltGraphDatabaseServiceSPI;
    private final Duration bookmarkAwaitDuration;
    private final Clock clock;
    private final BoltChannel boltChannel;
    private final StatementProcessorReleaseManager resourceReleaseManager;

    public TransactionStateMachineV1SPI( BoltGraphDatabaseServiceSPI boltGraphDatabaseServiceSPI, BoltChannel boltChannel, Duration bookmarkAwaitDuration,
            SystemNanoClock clock, StatementProcessorReleaseManager resourceReleaseManger )
    {
        this.boltGraphDatabaseServiceSPI = boltGraphDatabaseServiceSPI;
        this.boltChannel = boltChannel;
        this.bookmarkAwaitDuration = bookmarkAwaitDuration;
        this.clock = clock;
        this.resourceReleaseManager = resourceReleaseManger;
    }

    @Override
    public void awaitUpToDate( List<Bookmark> bookmarks )
    {
        if ( !bookmarks.isEmpty() && bookmarks.size() != 1 )
        {
            throw new IllegalArgumentException( "Expected zero or one bookmark. Received: " + bookmarks );
        }
        awaitAllBookmarks( bookmarks );
    }

    @Override
    public Bookmark newestBookmark()
    {
        var txId = newestEncounteredTxId();
        return new BookmarkWithPrefix( txId );
    }

    protected long newestEncounteredTxId()
    {
        return boltGraphDatabaseServiceSPI.newestEncounteredTxId();
    }

    @Override
    public BoltTransaction beginTransaction( LoginContext loginContext, Duration txTimeout, AccessMode accessMode, Map<String,Object> txMetadata )
    {
        return boltGraphDatabaseServiceSPI.beginTransaction( explicit, loginContext, boltChannel.info(), txTimeout, accessMode, txMetadata  );
    }

    @Override
    public BoltTransaction beginPeriodicCommitTransaction( LoginContext loginContext, Duration txTimeout, AccessMode accessMode, Map<String,Object> txMetadata )
    {
        return boltGraphDatabaseServiceSPI.beginTransaction( implicit, loginContext, boltChannel.info(), txTimeout, accessMode, txMetadata  );
    }

    @Override
    public void bindTransactionToCurrentThread( BoltTransaction tx )
    {
        tx.bindToCurrentThread();
    }

    @Override
    public void unbindTransactionFromCurrentThread( BoltTransaction tx )
    {

        if ( tx != null )
        {
            tx.unbindFromCurrentThread();
        }
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

    protected BoltResultHandle newBoltResultHandle( String statement, MapValue params, BoltQueryExecutor boltQueryExecutor )
    {
        return new BoltResultHandleV1( statement, params, boltQueryExecutor );
    }

    protected final void awaitAllBookmarks( List<Bookmark> bookmarks )
    {
        boltGraphDatabaseServiceSPI.awaitUpToDate( bookmarks, bookmarkAwaitDuration );
    }

    public class BoltResultHandleV1 implements BoltResultHandle
    {
        private final String statement;
        private final MapValue params;
        private final BoltQueryExecutor boltQueryExecutor;
        private BoltQueryExecution boltQueryExecution;

        public BoltResultHandleV1( String statement, MapValue params, BoltQueryExecutor boltQueryExecutor )
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

        protected BoltResult newBoltResult( QueryExecution result,
                BoltAdapterSubscriber subscriber, Clock clock )
        {
            return new CypherAdapterStream( result, subscriber, clock );
        }

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

    public static class BoltAdapterSubscriber implements QuerySubscriber
    {
        private BoltResult.RecordConsumer recordConsumer;
        private Throwable error;
        private QueryStatistics statistics;
        private int numberOfFields;

        @Override
        public void onResult( int numberOfFields )
        {
            this.numberOfFields = numberOfFields;
        }

        @Override
        public void onRecord() throws IOException
        {
            recordConsumer.beginRecord( numberOfFields );
        }

        @Override
        public void onField( AnyValue value ) throws IOException
        {
            recordConsumer.consumeField( value );
        }

        @Override
        public void onRecordCompleted() throws Exception
        {
            recordConsumer.endRecord();
        }

        @Override
        public void onError( Throwable throwable )
        {
            if ( this.error == null )
            {
                this.error = throwable;
            }
            //error might occur before the recordConsumer was initialized
            if ( recordConsumer != null )
            {
                recordConsumer.onError();
            }
        }

        @Override
        public void onResultCompleted( QueryStatistics statistics )
        {
            this.statistics = statistics;
        }

        QueryStatistics queryStatistics()
        {
            return statistics;
        }

        void setRecordConsumer( BoltResult.RecordConsumer recordConsumer )
        {
            this.recordConsumer = recordConsumer;
        }

        void assertSucceeded() throws KernelException
        {
            if ( error != null )
            {
                if ( error instanceof KernelException )
                {
                    throw (KernelException) error;
                }
                else if ( error instanceof Status.HasStatus )
                {
                    throw new QueryExecutionKernelException( (Throwable & Status.HasStatus) error );
                }
                else
                {
                    throw new QueryExecutionKernelException( new CypherExecutionException( error.getMessage(), error ) );
                }
            }
        }
    }
}
