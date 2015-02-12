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
package org.neo4j.kernel.impl.api.index;

import org.junit.Test;

import java.io.IOException;
import java.util.Collections;

import org.neo4j.kernel.api.index.IndexEntryConflictException;
import org.neo4j.kernel.api.index.IndexUpdater;
import org.neo4j.kernel.api.index.NodePropertyUpdate;
import org.neo4j.kernel.api.index.PreparedIndexUpdates;
import org.neo4j.test.DoubleLatch;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.anyCollection;
import static org.mockito.Matchers.startsWith;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
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
    public void shouldNotPrepareBeforeCreate() throws IOException, IndexEntryConflictException
    {
        // GIVEN
        IndexProxy inner = mockIndexProxy();
        IndexProxy outer = newContractCheckingIndexProxy( inner );

        // WHEN
        IndexUpdater updater = outer.newUpdater( IndexUpdateMode.ONLINE );
        updater.prepare( null );
    }

    @Test(expected = IllegalStateException.class)
    public void shouldNotPrepareAfterClose() throws IOException, IndexEntryConflictException
    {
        // GIVEN
        IndexProxy inner = mockIndexProxy();
        IndexProxy outer = newContractCheckingIndexProxy( inner );

        // WHEN
        outer.start();
        outer.close();
        IndexUpdater updater = outer.newUpdater( IndexUpdateMode.ONLINE );
        updater.prepare( null );
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
    public void shouldNotCloseWhilePreparing() throws IOException
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
                try
                {
                    IndexUpdater updater = outer.newUpdater( IndexUpdateMode.ONLINE );
                    updater.prepare( null );
                    latch.startAndAwaitFinish();
                }
                catch ( IndexEntryConflictException e )
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

    @Test
    public void updaterShouldCloseOnCommit() throws IOException, IndexEntryConflictException
    {
        // Given
        ContractCheckingIndexProxy proxy = newContractCheckingIndexProxy( mockIndexProxy() );
        proxy.start();

        // When
        // new updater is created and prepared updates are committed
        IndexUpdater updater = proxy.newUpdater( IndexUpdateMode.ONLINE );
        updater.prepare( Collections.<NodePropertyUpdate>emptyList() ).commit();

        // Then
        // can close index proxy as there are no updates in progress
        proxy.close();
    }

    @Test
    public void updaterShouldCloseOnRollback() throws IOException, IndexEntryConflictException
    {
        // Given
        ContractCheckingIndexProxy proxy = newContractCheckingIndexProxy( mockIndexProxy() );
        proxy.start();

        // When
        // new updater is created and prepared updates are rolled back
        IndexUpdater updater = proxy.newUpdater( IndexUpdateMode.ONLINE );
        updater.prepare( Collections.<NodePropertyUpdate>emptyList() ).rollback();

        // Then
        // can close index proxy as there are no updates in progress
        proxy.close();
    }

    @Test
    @SuppressWarnings( "unchecked" )
    public void updaterShouldCloseEvenIfDelegatePrepareThrows() throws Exception
    {
        // Given
        // index updater delegate that throws
        IndexUpdater throwingUpdater = mock( IndexUpdater.class );
        when( throwingUpdater.prepare( anyCollection() ) ).thenThrow( IOException.class );
        ContractCheckingIndexProxy proxy = newContractCheckingIndexProxy( mockIndexProxy( throwingUpdater ) );
        proxy.start();

        // When
        // new updater is created and prepared updates are committed
        IndexUpdater updater = proxy.newUpdater( IndexUpdateMode.ONLINE );
        try
        {
            updater.prepare( Collections.<NodePropertyUpdate>emptyList() );
            fail( "Should have thrown " + IOException.class.getSimpleName() );
        }
        catch ( IOException ignored )
        {
        }

        // Then
        // can close index proxy as there are no updates in progress
        proxy.close();
    }

    @Test
    @SuppressWarnings( "unchecked" )
    public void updaterShouldCloseEvenIfDelegateCommitThrows() throws Exception
    {
        // Given
        // index updater delegate that throws
        IndexUpdater updaterMock = mock( IndexUpdater.class );
        PreparedIndexUpdates updates = mock( PreparedIndexUpdates.class );
        doThrow( IOException.class ).when( updates ).commit();
        when( updaterMock.prepare( anyCollection() ) ).thenReturn( updates );
        ContractCheckingIndexProxy proxy = newContractCheckingIndexProxy( mockIndexProxy( updaterMock ) );
        proxy.start();

        // When
        // new updater is created and prepared updates are committed
        IndexUpdater updater = proxy.newUpdater( IndexUpdateMode.ONLINE );
        PreparedIndexUpdates prepared = updater.prepare( Collections.<NodePropertyUpdate>emptyList() );
        try
        {
            prepared.commit();
            fail( "Should have thrown " + IOException.class.getSimpleName() );
        }
        catch ( IOException ignored )
        {
        }

        // Then
        // can close index proxy as there are no updates in progress
        proxy.close();
    }

    @Test
    @SuppressWarnings( "unchecked" )
    public void updaterShouldCloseEvenIfDelegateRollbackThrows() throws Exception
    {
        // Given
        // index updater delegate that throws
        IndexUpdater updaterMock = mock( IndexUpdater.class );
        PreparedIndexUpdates updates = mock( PreparedIndexUpdates.class );
        doThrow( IllegalArgumentException.class ).when( updates ).rollback();
        when( updaterMock.prepare( anyCollection() ) ).thenReturn( updates );
        ContractCheckingIndexProxy proxy = newContractCheckingIndexProxy( mockIndexProxy( updaterMock ) );
        proxy.start();

        // When
        // new updater is created and prepared updates are committed
        IndexUpdater updater = proxy.newUpdater( IndexUpdateMode.ONLINE );
        PreparedIndexUpdates prepared = updater.prepare( Collections.<NodePropertyUpdate>emptyList() );
        try
        {
            prepared.rollback();
            fail( "Should have thrown " + IllegalArgumentException.class.getSimpleName() );
        }
        catch ( IllegalArgumentException ignored )
        {
        }

        // Then
        // can close index proxy as there are no updates in progress
        proxy.close();
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
