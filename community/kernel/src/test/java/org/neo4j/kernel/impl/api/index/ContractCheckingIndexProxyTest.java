/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.kernel.impl.api.index;

import java.io.IOException;

import org.junit.Test;

import org.neo4j.kernel.api.exceptions.index.IndexCapacityExceededException;
import org.neo4j.kernel.api.index.IndexEntryConflictException;
import org.neo4j.kernel.api.index.IndexUpdater;
import org.neo4j.test.DoubleLatch;

import static org.neo4j.kernel.impl.api.index.SchemaIndexTestHelper.mockIndexProxy;

public class ContractCheckingIndexProxyTest
{
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


    @Test(expected = IllegalStateException.class)
    public void shouldNotUpdateBeforeCreate() throws Exception
    {
        // GIVEN
        IndexProxy inner = mockIndexProxy();
        IndexProxy outer = newContractCheckingIndexProxy( inner );

        // WHEN
        try (IndexUpdater updater = outer.newUpdater( IndexUpdateMode.ONLINE ) )
        {
            updater.process( null );
        }
    }

    @Test(expected = IllegalStateException.class)
    public void shouldNotUpdateAfterClose() throws Exception
    {
        // GIVEN
        IndexProxy inner = mockIndexProxy();
        IndexProxy outer = newContractCheckingIndexProxy( inner );

        // WHEN
        outer.start();
        outer.close();
        try (IndexUpdater updater = outer.newUpdater( IndexUpdateMode.ONLINE ))
        {
            updater.process( null );
        }
    }

    @Test(expected = IllegalStateException.class)
    public void shouldNotForceBeforeCreate() throws IOException
    {
        // GIVEN
        IndexProxy inner = mockIndexProxy();
        IndexProxy outer = newContractCheckingIndexProxy( inner );

        // WHEN
        outer.force();
    }

    @Test(expected = IllegalStateException.class)
    public void shouldNotForceAfterClose() throws IOException
    {
        // GIVEN
        IndexProxy inner = mockIndexProxy();
        IndexProxy outer = newContractCheckingIndexProxy( inner );

        // WHEN
        outer.start();
        outer.close();
        outer.force();
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
                latch.startAndAwaitFinish();
            }
        };
        final IndexProxy outer = newContractCheckingIndexProxy( inner );

        // WHEN
        runInSeparateThread( new ThrowingRunnable()
        {
            @Override
            public void run() throws IOException
            {
                outer.start();
            }
        } );

        try
        {
            latch.awaitStart();
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
                latch.startAndAwaitFinish();
            }
        };
        final IndexProxy outer = newContractCheckingIndexProxy( inner );

        // WHEN
        runInSeparateThread( new ThrowingRunnable()
        {
            @Override
            public void run() throws IOException
            {
                outer.start();
            }
        } );

        try
        {
            latch.awaitStart();
            outer.drop();
        }
        finally
        {
            latch.finish();
        }
    }


    @Test( expected = /* THEN */ IllegalStateException.class )
    public void shouldNotCloseWhileUpdating() throws IOException
    {
        // GIVEN
        final DoubleLatch latch = new DoubleLatch();
        final IndexProxy inner = new IndexProxyAdapter()
        {
            @Override
            public IndexUpdater newUpdater( IndexUpdateMode mode )
            {
                return super.newUpdater( mode );
            }
        };
        final IndexProxy outer = newContractCheckingIndexProxy( inner );
        outer.start();

        // WHEN
        runInSeparateThread( new ThrowingRunnable()
        {
            @Override
            public void run() throws IOException
            {
                try (IndexUpdater updater = outer.newUpdater( IndexUpdateMode.ONLINE ))
                {
                    updater.process( null );
                    latch.startAndAwaitFinish();
                }
                catch ( IndexEntryConflictException | IndexCapacityExceededException e )
                {
                    throw new RuntimeException( e );
                }
            }
        } );

        try
        {
            latch.awaitStart();
            outer.close();
        }
        finally
        {
            latch.finish();
        }
    }

    @Test( expected = /* THEN */ IllegalStateException.class )
    public void shouldNotCloseWhileForcing() throws IOException
    {
        // GIVEN
        final DoubleLatch latch = new DoubleLatch();
        final IndexProxy inner = new IndexProxyAdapter()
        {
            @Override
            public void force()
            {
                latch.startAndAwaitFinish();
            }
        };
        final IndexProxy outer = newContractCheckingIndexProxy( inner );
        outer.start();

        // WHEN
        runInSeparateThread( new ThrowingRunnable()
        {
            @Override
            public void run() throws IOException
            {
                outer.force();
            }
        } );

        try
        {
            latch.awaitStart();
            outer.close();
        }
        finally
        {
            latch.finish();
        }
    }

    private interface ThrowingRunnable
    {
        void run() throws IOException;
    }

    private void runInSeparateThread( final ThrowingRunnable action )
    {
        new Thread( new Runnable()
        {
            @Override
            public void run()
            {
                try
                {
                    action.run();
                }
                catch ( IOException e )
                {
                    throw new RuntimeException( e );
                }
            }
        } ).start();
    }

    private ContractCheckingIndexProxy newContractCheckingIndexProxy( IndexProxy inner )
    {
        return new ContractCheckingIndexProxy( inner, false );
    }
}
