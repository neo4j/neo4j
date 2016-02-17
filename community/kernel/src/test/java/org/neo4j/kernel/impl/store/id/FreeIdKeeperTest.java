/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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
package org.neo4j.kernel.impl.store.id;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyZeroInteractions;
import static org.neo4j.kernel.impl.store.id.FreeIdKeeper.NO_RESULT;

import java.io.File;
import java.nio.ByteBuffer;
import java.util.HashSet;
import java.util.Set;

import org.junit.Test;
import org.neo4j.graphdb.mockfs.EphemeralFileSystemAbstraction;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.fs.StoreChannel;

public class FreeIdKeeperTest
{
    @Test
    public void newlyConstructedInstanceShouldReportProperDefaultValues() throws Exception
    {
        // Given
        StoreChannel channel = mock( StoreChannel.class );
        int threshold = 10;
        FreeIdKeeper keeper = new FreeIdKeeper( channel, threshold, true );

        // when
        // then
        assertEquals( NO_RESULT, keeper.getId() );
        assertEquals( 0, keeper.getCount() );
    }

    @Test
    public void freeingAnIdShouldReturnThatIdAndUpdateTheCountWhenAggressiveReuseIsSet() throws Exception
    {
        // Given
        StoreChannel channel = mock( StoreChannel.class );
        int threshold = 10;
        FreeIdKeeper keeper = new FreeIdKeeper( channel, threshold, true );

        // when
        keeper.freeId( 13 );

        // then
        assertEquals( 1, keeper.getCount() );

        // when
        long result = keeper.getId();

        // then
        assertEquals( 13, result );
        assertEquals( 0, keeper.getCount() );
    }

    @Test
    public void shouldReturnMinusOneWhenRunningOutOfIds() throws Exception
    {
        // Given
        StoreChannel channel = mock( StoreChannel.class );
        int threshold = 10;
        FreeIdKeeper keeper = new FreeIdKeeper( channel, threshold, true );

        // when
        keeper.freeId( 13 );

        // then
        assertEquals( 13, keeper.getId() );
        assertEquals( NO_RESULT, keeper.getId() );
        assertEquals( NO_RESULT, keeper.getId() );
    }

    @Test
    public void shouldOnlyOverflowWhenThresholdIsReached() throws Exception
    {
        // Given
        FileSystemAbstraction fs = new EphemeralFileSystemAbstraction();
        StoreChannel channel = spy( fs.open( new File( "id.file" ), "rw" ) );

        int threshold = 10;
        FreeIdKeeper keeper = new FreeIdKeeper( channel, threshold, true );
        reset( channel ); // because we get the position in the constructor, we need to reset all calls on the spy

        // when
        // we free 9 ids
        for ( int i = 0; i < threshold - 1; i++ )
        {
            keeper.freeId( i );
        }

        // then
        verifyZeroInteractions( channel );

        // when we free one more
        keeper.freeId( 10 );

        // then
        verify( channel ).write( any( ByteBuffer.class ) );
    }

    @Test
    public void shouldReadBackPersistedIdsWhenAggressiveReuseIsSet() throws Exception
    {
        // given
        FileSystemAbstraction fs = new EphemeralFileSystemAbstraction();
        StoreChannel channel = fs.open( new File( "id.file" ), "rw" );

        int threshold = 10;
        FreeIdKeeper keeper = new FreeIdKeeper( channel, threshold, true );

        // when
        // we store enough ids to cause overflow to file
        for ( int i = 0; i < threshold; i++ )
        {
            keeper.freeId( i );
        }

        // then
        // they should be returned in order
        for ( int i = 0; i < threshold; i++ )
        {
            assertEquals( i, keeper.getId()) ;
        }
    }

    @Test
    public void shouldReadBackManyPersistedIdBatchesWhenAggressiveReuseIsSet() throws Exception
    {
        // given
        FileSystemAbstraction fs = new EphemeralFileSystemAbstraction();
        StoreChannel channel = fs.open( new File( "id.file" ), "rw" );

        int threshold = 10;
        FreeIdKeeper keeper = new FreeIdKeeper( channel, threshold, true );
        Set<Long> freeIds = new HashSet<>();

        // when
        // we store enough ids to cause overflow to file, in two batches
        for ( long i = 0; i < threshold * 2; i++ )
        {
            keeper.freeId( i );
            freeIds.add( i );
        }

        // then
        // they should be returned
        assertEquals( freeIds.size(), keeper.getCount() );
        for ( int i = threshold * 2 - 1; i >= 0; i-- )
        {
            assertTrue( freeIds.remove( keeper.getId() ) );
        }
    }

    @Test
    public void shouldFirstReturnNonPersistedIdsAndThenPersistedOnesWhenAggressiveReuse() throws Exception
    {
        // this is testing the stack property, but from the viewpoint of avoiding unnecessary disk reads
        // given
        FileSystemAbstraction fs = new EphemeralFileSystemAbstraction();
        StoreChannel channel = fs.open( new File( "id.file" ), "rw" );

        int threshold = 10;
        FreeIdKeeper keeper = new FreeIdKeeper( channel, threshold, true );

        // when
        // we store enough ids to cause overflow to file
        for ( int i = 0; i < threshold; i++ )
        {
            keeper.freeId( i );
        }
        // and then some more
        int extraIds = 3;
        for ( int i = threshold; i < threshold + extraIds; i++ )
        {
            keeper.freeId( i );
        }

        // then
        // the first returned should be the newly freed ones
        for ( int i = threshold; i < threshold + extraIds ; i++ )
        {
            assertEquals( i, keeper.getId() ) ;
        }
        // and then there should be the persisted ones
        for ( int i = 0; i < threshold ; i++ )
        {
            assertEquals( i, keeper.getId() ) ;
        }

    }

    @Test
    public void persistedIdsShouldStillBeCounted() throws Exception
    {
        // given
        FileSystemAbstraction fs = new EphemeralFileSystemAbstraction();
        StoreChannel channel = fs.open( new File( "id.file" ), "rw" );

        int threshold = 10;
        FreeIdKeeper keeper = new FreeIdKeeper( channel, threshold, true );

        // when
        // we store enough ids to cause overflow to file
        for ( int i = 0; i < threshold; i++ )
        {
            keeper.freeId( i );
        }
        // and then some more
        int extraIds = 3;
        for ( int i = threshold; i < threshold + extraIds; i++ )
        {
            keeper.freeId( i );
        }

        // then
        // the count should be returned correctly
        assertEquals( threshold + extraIds, keeper.getCount() );
    }

    @Test
    public void shouldStoreAndRestoreIds() throws Exception
    {
        // given
        FileSystemAbstraction fs = new EphemeralFileSystemAbstraction();
        StoreChannel channel = fs.open( new File( "id.file" ), "rw" );

        int threshold = 10;
        FreeIdKeeper keeper = new FreeIdKeeper( channel, threshold, true );
        Set<Long> freeIds = new HashSet<>(); // stack guarantees are not maintained between restarts

        // when
        // we store enough ids to cause overflow to file
        for ( long i = 0; i < threshold; i++ )
        {
            keeper.freeId( i );
            freeIds.add( i );
        }
        // and then some more
        int extraIds = 3;
        for ( long i = threshold; i < threshold + extraIds; i++ )
        {
            keeper.freeId( i );
            freeIds.add( i );
        }
        // and then we close the keeper
        keeper.close();
        channel.close();
        // and then we open a new one over the same file
        channel = fs.open( new File( "id.file" ), "rw" );
        keeper = new FreeIdKeeper( channel, threshold, true );

        // then
        // the count should be returned correctly
        assertEquals( threshold + extraIds, keeper.getCount() );
        assertEquals( freeIds.size(), keeper.getCount() );
        // and the ids, including the ones that did not cause a write, are still there (as a stack)
        for ( int i = threshold + extraIds - 1; i >= 0; i-- )
        {
            long id = keeper.getId();
            assertTrue( freeIds.contains( id ) );
        }
    }

    @Test
    public void shouldNotReturnNewlyReleasedIdsIfAggressiveIsFalse() throws Exception
    {
        // given
        FileSystemAbstraction fs = new EphemeralFileSystemAbstraction();
        StoreChannel channel = fs.open( new File( "id.file" ), "rw" );

        int threshold = 10;
        FreeIdKeeper keeper = new FreeIdKeeper( channel, threshold, false );

        // when
        keeper.freeId( 1 );
        long nextFree = keeper.getId();

        // then
        assertEquals( NO_RESULT, nextFree );
    }

    @Test
    public void shouldNotReturnIdsPersistedDuringThisRunIfAggressiveIsFalse() throws Exception
    {
        // given
        FileSystemAbstraction fs = new EphemeralFileSystemAbstraction();
        StoreChannel channel = spy( fs.open( new File( "id.file" ), "rw" ) );

        int threshold = 10;
        FreeIdKeeper keeper = new FreeIdKeeper( channel, threshold, false );

        // when
        // enough ids are persisted to overflow
        for ( int i = 0; i < threshold; i++ )
        {
            keeper.freeId( i );
        }

        // then
        // stuff must have been written to disk
        verify( channel, times( 1 ) ).write( any( ByteBuffer.class ) );
        // and no ids can be returned
        assertEquals( NO_RESULT, keeper.getId() );
    }

    @Test
    public void shouldReturnIdsRestoredAndIgnoreNewlyReleasedIfAggressiveReuseIsFalse() throws Exception
    {
        // given
        FileSystemAbstraction fs = new EphemeralFileSystemAbstraction();
        StoreChannel channel = fs.open( new File("id.file" ), "rw" );

        int threshold = 10;
        FreeIdKeeper keeper = new FreeIdKeeper( channel, threshold, false );
        Set<Long> freeIds = new HashSet<>();
        for ( long i = 0; i < threshold; i++ )
        {
            keeper.freeId( i );
            freeIds.add( i );
        }
        keeper.close();
        channel.close();
        // and then we open a new one over the same file
        channel = fs.open( new File( "id.file" ), "rw" );
        keeper = new FreeIdKeeper( channel, threshold, false );

        // when
        // we release some ids that spill to disk
        for ( int i = 0; i < threshold; i++ )
        {
            keeper.freeId( i );
        }

        // then
        // we should retrieve all ids restored
        for ( int i = 0; i < threshold; i++ )
        {
            assertTrue( freeIds.remove( keeper.getId() ) );
        }

        // then
        // we should have no ids to return
        assertEquals( NO_RESULT, keeper.getId() );
    }

    @Test
    public void shouldReturnNoResultIfIdsAreRestoredAndExhaustedAndThereAreFreeIdsFromThisRunWithAggressiveFalse()
            throws Exception
    {
        // given
        FileSystemAbstraction fs = new EphemeralFileSystemAbstraction();
        StoreChannel channel = fs.open( new File("id.file" ), "rw" );

        int threshold = 10;
        FreeIdKeeper keeper = new FreeIdKeeper( channel, threshold, false );
        Set<Long> freeIds = new HashSet<>();
        for ( long i = 0; i < threshold; i++ )
        {
            keeper.freeId( i );
            freeIds.add( i );
        }
        keeper.close();
        channel.close();
        // and then we open a new one over the same file
        channel = fs.open( new File( "id.file" ), "rw" );
        keeper = new FreeIdKeeper( channel, threshold, false );

        // when - then
        // we exhaust all ids restored
        for ( int i = 0; i < threshold; i++ )
        {
            assertTrue( freeIds.remove( keeper.getId() ) );
        }

        // when
        // we release some ids that spill to disk
        for ( int i = 0; i < threshold; i++ )
        {
            keeper.freeId( i );
        }

        // then
        // we should have no ids to return
        assertEquals( NO_RESULT, keeper.getId() );
    }
}
