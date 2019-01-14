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
package org.neo4j.kernel.impl.api.index;

import org.junit.Test;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;

import org.neo4j.io.pagecache.IOLimiter;
import org.neo4j.kernel.api.exceptions.index.IndexEntryConflictException;
import org.neo4j.kernel.api.index.IndexUpdater;
import org.neo4j.test.DoubleLatch;
import org.neo4j.test.ThreadTestUtils;

import static org.neo4j.kernel.impl.api.index.SchemaIndexTestHelper.mockIndexProxy;

public class ContractCheckingIndexProxyTest
{
    private static final long TEST_TIMEOUT = 20_000;

    @Test( expected = /* THEN */ IllegalStateException.class )
    public void shouldNotCreateIndexTwice() throws IOException
    {
        // GIVEN
        IndexProxy inner = mockIndexProxy();
        IndexProxy outer = newContractCheckingIndexProxy( inner );

        // WHEN
        outer.start();
        outer.start();
    }

    @Test( expected = /* THEN */ IllegalStateException.class )
    public void shouldNotCloseIndexTwice() throws IOException
    {
        // GIVEN
        IndexProxy inner = mockIndexProxy();
        IndexProxy outer = newContractCheckingIndexProxy( inner );

        // WHEN
        outer.close();
        outer.close();
    }

    @Test( expected = /* THEN */ IllegalStateException.class )
    public void shouldNotDropIndexTwice() throws IOException
    {
        // GIVEN
        IndexProxy inner = mockIndexProxy();
        IndexProxy outer = newContractCheckingIndexProxy( inner );

        // WHEN
        outer.drop();
        outer.drop();
    }

    @Test( expected = /* THEN */ IllegalStateException.class )
    public void shouldNotDropAfterClose() throws IOException
    {
        // GIVEN
        IndexProxy inner = mockIndexProxy();
        IndexProxy outer = newContractCheckingIndexProxy( inner );

        // WHEN
        outer.close();
        outer.drop();
    }

    @Test
    public void shouldDropAfterCreate() throws IOException
    {
        // GIVEN
        IndexProxy inner = mockIndexProxy();
        IndexProxy outer = newContractCheckingIndexProxy( inner );

        // WHEN
        outer.start();

        // PASS
        outer.drop();
    }

    @Test
    public void shouldCloseAfterCreate() throws IOException
    {
        // GIVEN
        IndexProxy inner = mockIndexProxy();
        IndexProxy outer = newContractCheckingIndexProxy( inner );

        // WHEN
        outer.start();

        // PASS
        outer.close();
    }

    @Test( expected = IllegalStateException.class )
    public void shouldNotUpdateBeforeCreate() throws Exception
    {
        // GIVEN
        IndexProxy inner = mockIndexProxy();
        IndexProxy outer = newContractCheckingIndexProxy( inner );

        // WHEN
        try ( IndexUpdater updater = outer.newUpdater( IndexUpdateMode.ONLINE ) )
        {
            updater.process( null );
        }
    }

    @Test( expected = IllegalStateException.class )
    public void shouldNotUpdateAfterClose() throws Exception
    {
        // GIVEN
        IndexProxy inner = mockIndexProxy();
        IndexProxy outer = newContractCheckingIndexProxy( inner );

        // WHEN
        outer.start();
        outer.close();
        try ( IndexUpdater updater = outer.newUpdater( IndexUpdateMode.ONLINE ) )
        {
            updater.process( null );
        }
    }

    @Test( expected = IllegalStateException.class )
    public void shouldNotForceBeforeCreate() throws IOException
    {
        // GIVEN
        IndexProxy inner = mockIndexProxy();
        IndexProxy outer = newContractCheckingIndexProxy( inner );

        // WHEN
        outer.force( IOLimiter.unlimited() );
    }

    @Test( expected = IllegalStateException.class )
    public void shouldNotForceAfterClose() throws IOException
    {
        // GIVEN
        IndexProxy inner = mockIndexProxy();
        IndexProxy outer = newContractCheckingIndexProxy( inner );

        // WHEN
        outer.start();
        outer.close();
        outer.force( IOLimiter.unlimited() );
    }

    @Test( expected = /* THEN */ IllegalStateException.class )
    public void shouldNotCloseWhileCreating() throws IOException
    {
        // GIVEN
        final DoubleLatch latch = new DoubleLatch();
        final IndexProxy inner = new IndexProxyAdapter()
        {
            @Override
            public void start()
            {
                latch.startAndWaitForAllToStartAndFinish();
            }
        };
        final IndexProxy outer = newContractCheckingIndexProxy( inner );

        // WHEN
        runInSeparateThread( outer::start );

        try
        {
            latch.waitForAllToStart();
            outer.close();
        }
        finally
        {
            latch.finish();
        }
    }

    @Test( expected = /* THEN */ IllegalStateException.class )
    public void shouldNotDropWhileCreating() throws IOException
    {
        // GIVEN
        final DoubleLatch latch = new DoubleLatch();
        final IndexProxy inner = new IndexProxyAdapter()
        {
            @Override
            public void start()
            {
                latch.startAndWaitForAllToStartAndFinish();
            }
        };
        final IndexProxy outer = newContractCheckingIndexProxy( inner );

        // WHEN
        runInSeparateThread( outer::start );

        try
        {
            latch.waitForAllToStart();
            outer.drop();
        }
        finally
        {
            latch.finish();
        }
    }

    @Test( timeout = TEST_TIMEOUT )
    public void closeWaitForUpdateToFinish() throws IOException, InterruptedException
    {
        // GIVEN
        CountDownLatch latch = new CountDownLatch( 1 );
        final IndexProxy inner = new IndexProxyAdapter()
        {
            @Override
            public IndexUpdater newUpdater( IndexUpdateMode mode )
            {
                return super.newUpdater( mode );
            }
        };
        final IndexProxy outer = newContractCheckingIndexProxy( inner );
        Thread actionThread = createActionThread( outer::close );
        outer.start();

        // WHEN
        Thread updaterThread = runInSeparateThread( () ->
        {
            try ( IndexUpdater updater = outer.newUpdater( IndexUpdateMode.ONLINE ) )
            {
                updater.process( null );
                try
                {
                    actionThread.start();
                    latch.await();
                }
                catch ( InterruptedException e )
                {
                    throw new RuntimeException( e );
                }
            }
            catch ( IndexEntryConflictException e )
            {
                throw new RuntimeException( e );
            }
        } );

        ThreadTestUtils.awaitThreadState( actionThread, TEST_TIMEOUT, Thread.State.TIMED_WAITING );
        latch.countDown();
        updaterThread.join();
        actionThread.join();
    }

    @Test( timeout = TEST_TIMEOUT )
    public void closeWaitForForceToComplete() throws Exception
    {
        // GIVEN
        CountDownLatch latch = new CountDownLatch( 1 );
        AtomicReference<Thread> actionThreadReference = new AtomicReference<>();
        final IndexProxy inner = new IndexProxyAdapter()
        {
            @Override
            public void force( IOLimiter ioLimiter )
            {
                try
                {
                    actionThreadReference.get().start();
                    latch.await();
                }
                catch ( Exception e )
                {
                  throw new RuntimeException( e );
                }
            }
        };
        IndexProxy outer = newContractCheckingIndexProxy( inner );
        Thread actionThread = createActionThread( outer::close );
        actionThreadReference.set( actionThread );

        outer.start();
        Thread thread = runInSeparateThread( () -> outer.force( IOLimiter.unlimited() ) );

        ThreadTestUtils.awaitThreadState( actionThread, TEST_TIMEOUT, Thread.State.TIMED_WAITING );
        latch.countDown();

        thread.join();
        actionThread.join();
    }

    private interface ThrowingRunnable
    {
        void run() throws IOException;
    }

    private Thread runInSeparateThread( final ThrowingRunnable action )
    {
        Thread thread = createActionThread( action );
        thread.start();
        return thread;
    }

    private Thread createActionThread( ThrowingRunnable action )
    {
        return new Thread( () ->
        {
            try
            {
                action.run();
            }
            catch ( IOException e )
            {
                throw new RuntimeException( e );
            }
        } );
    }

    private ContractCheckingIndexProxy newContractCheckingIndexProxy( IndexProxy inner )
    {
        return new ContractCheckingIndexProxy( inner, false );
    }
}
