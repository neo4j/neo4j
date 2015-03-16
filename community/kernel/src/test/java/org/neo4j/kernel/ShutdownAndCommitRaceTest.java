/**
 * Copyright (c) 2002-2015 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
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
package org.neo4j.kernel;

import org.junit.Rule;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.lang.Thread.State;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Transaction;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.PagedFile;
import org.neo4j.kernel.AvailabilityGuard;
import org.neo4j.kernel.DatabaseAvailability;
import org.neo4j.kernel.NeoStoreDataSource;
import org.neo4j.kernel.api.exceptions.TransactionFailureException;
import org.neo4j.kernel.impl.store.NeoStore;
import org.neo4j.kernel.impl.transaction.log.LogRotationControl;
import org.neo4j.test.Barrier;
import org.neo4j.test.ImpermanentGraphDatabase;
import org.neo4j.test.OtherThreadExecutor.WorkerCommand;
import org.neo4j.test.OtherThreadRule;

import static org.junit.Assert.fail;

import static org.neo4j.helpers.Exceptions.contains;

public class ShutdownAndCommitRaceTest
{
    /**
     * There was a window of opportunity in shutdown ({@link NeoStoreDataSource#stop()} specifically)
     * after {@link LogRotationControl#awaitAllTransactionsClosed() waiting for committing transactions to be closed}
     * and before {@link NeoStore#incrementAndGetVersion() the log version was incremented} where a transaction
     * that had gotten past the {@link AvailabilityGuard#checkAvailability(long, Class) availability guard} in
     * {@link GraphDatabaseService#beginTx()} could commit and apply its changes to store and return
     * OK to the user without the transaction being covered by the transaction log, since the log would already
     * have been rotated away, by the incremented log version. A crash after this point would not recover
     * this transaction. Actually, such a transaction could race with the shutdown thread all the way down to
     * {@link PagedFile#close() unmapping the stores} from the {@link PageCache}.
     *
     * This test asserts that the window described above doesn't exist anymore.
     */
    @Test
    public void shouldNotBeAbleToCommitingDuringOrAfterForcingWhileShuttingDown() throws Exception
    {
        // GIVEN a database with a controllable flushing of the page cache
        final AtomicBoolean enabled = new AtomicBoolean();
        final GraphDatabaseService db = new ImpermanentGraphDatabase()
        {
            @Override
            protected PageCache createPageCache()
            {
                return new ShutdownControlledPageCache( super.createPageCache(), enabled );
            }

            @Override
            protected void createDatabaseAvailability()
            {
                // Don't wait unnecessarily for open transactions (hence the 0 argument) to close,
                // it would just interfere with this test
                life.add( new DatabaseAvailability( availabilityGuard, transactionMonitor, 0 ) );
            }
        };

        // WHEN ensuring that the transaction begins properly, and pausing there...
        Future<Void> commit = committer.execute( createNode( db ) );
        beginBarrier.await();

        // ... and starting shut down that will pause around flushing the page cache, where the barriers
        // will allow the transaction to commit and complete there
        enabled.set( true );
        Future<Void> shutdown = closer.execute( shutdown( db ) );

        // THEN the transaction should not be able to do that
        try
        {
            expectTimeoutOrTransactionFailure( committer, commit );
        }
        finally
        {
            commitBarrier.reached();
        }
        shutdown.get();
        try
        {
            commit.get();
        }
        catch ( ExecutionException e )
        {
            if ( !contains( e, TransactionFailureException.class ) &&
                 !contains( e, org.neo4j.graphdb.TransactionFailureException.class ) )
            {
                throw e;
            }
        }
    }

    private void expectTimeoutOrTransactionFailure( OtherThreadRule<Void> thread, Future<Void> future )
            throws Exception
    {
        for ( int i = 0; i < 50; i++ )
        {
            try
            {
                future.get( 100, TimeUnit.MILLISECONDS );
            }
            catch ( TimeoutException e )
            {
                if ( thread.get().state() == State.RUNNABLE )
                {
                    // For whatever reason we haven't yet gotten into the desired state where we await a monitor
                    // or similar. Keep on running until we see that state
                    continue;
                }

                // We timed out and it looks like we're waiting to acquire a monitor or similar, good
                return;
            }
        }
        fail( "It looks like the transaction was able to commit while the database was shutting down" );
    }

    private WorkerCommand<Void,Void> shutdown( final GraphDatabaseService db )
    {
        return new WorkerCommand<Void,Void>()
        {
            @Override
            public Void doWork( Void state ) throws Exception
            {
                System.out.println( "shutting down" );
                db.shutdown();
                System.out.println( "shut down" );
                return null;
            }
        };
    }

    private WorkerCommand<Void,Void> createNode( final GraphDatabaseService db )
    {
        return new WorkerCommand<Void,Void>()
        {
            @Override
            public Void doWork( Void state ) throws Exception
            {
                try ( Transaction tx = db.beginTx() )
                {
                    beginBarrier.reached();
                    db.createNode();
                    tx.success();
                }
                return null;
            }
        };
    }

    /**
     * {@link PageCache} that will coordinate with a {@link Barrier} in {@link #flush()} if the call comes
     * from {@link NeoStoreDataSource#stop()}.
     */
    private class ShutdownControlledPageCache implements PageCache
    {
        private final PageCache delegate;
        private final AtomicBoolean enabled;

        public ShutdownControlledPageCache( PageCache delegate, AtomicBoolean enabled )
        {
            this.delegate = delegate;
            this.enabled = enabled;
        }

        @Override
        public PagedFile map( File file, int pageSize ) throws IOException
        {
            return delegate.map( file, pageSize );
        }

        @Override
        public void flush() throws IOException
        {
            if ( enabled.get() )
            {
                System.out.println( "flushing" );
                beginBarrier.release();
            }
            delegate.flush();
            if ( enabled.get() )
            {
                System.out.println( "flushed" );
                commitBarrier.awaitUninterruptibly();
                commitBarrier.release();
            }
        }

        @Override
        public void close() throws IOException
        {
            delegate.close();
        }

        @Override
        public int pageSize()
        {
            return delegate.pageSize();
        }

        @Override
        public int maxCachedPages()
        {
            return delegate.maxCachedPages();
        }
    }

    public final @Rule OtherThreadRule<Void> committer = new OtherThreadRule<>( "committer" );
    public final @Rule OtherThreadRule<Void> closer = new OtherThreadRule<>( "closer" );
    // Member fields of the test so that usage is very easily tracable. Easier to understand the test.
    private final Barrier.Control beginBarrier = new Barrier.Control(), commitBarrier = new Barrier.Control();
}
