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
package org.neo4j.kernel.impl.store.id;

import org.junit.Rule;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.neo4j.collection.primitive.PrimitiveLongCollections;
import org.neo4j.io.fs.OpenMode;
import org.neo4j.io.fs.StoreChannel;
import org.neo4j.test.rule.fs.EphemeralFileSystemRule;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyZeroInteractions;
import static org.neo4j.kernel.impl.store.id.IdContainer.NO_RESULT;

public class FreeIdKeeperTest
{
    @Rule
    public final EphemeralFileSystemRule fs = new EphemeralFileSystemRule();

    @Test
    public void newlyConstructedInstanceShouldReportProperDefaultValues() throws Exception
    {
        // Given
        FreeIdKeeper keeper = getFreeIdKeeperAggressive();

        // then
        assertEquals( NO_RESULT, keeper.getId() );
        assertEquals( 0, keeper.getCount() );
    }

    @Test
    public void freeingAnIdShouldReturnThatIdAndUpdateTheCountWhenAggressiveModeIsSet() throws Exception
    {
        // Given
        FreeIdKeeper keeper = getFreeIdKeeperAggressive();

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
        FreeIdKeeper keeper = getFreeIdKeeperAggressive();

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
        StoreChannel channel = spy( fs.get().open( new File( "id.file" ), OpenMode.READ_WRITE ) );

        int batchSize = 10;
        FreeIdKeeper keeper = getFreeIdKeeperAggressive( channel, batchSize );
        reset( channel ); // because we get the position in the constructor, we need to reset all calls on the spy

        // when
        // we free 9 ids
        for ( int i = 0; i < batchSize - 1; i++ )
        {
            keeper.freeId( i );
        }

        // then
        verifyZeroInteractions( channel );

        // when we free one more
        keeper.freeId( 10 );

        // then
        verify( channel ).writeAll( any( ByteBuffer.class ) );
    }

    @Test
    public void shouldReadBackPersistedIdsWhenAggressiveModeIsSet() throws Exception
    {
        // given
        StoreChannel channel = getStoreChannel();

        int batchSize = 10;
        FreeIdKeeper keeper = getFreeIdKeeperAggressive( channel, batchSize );

        // when
        // we store enough ids to cause overflow to file
        for ( int i = 0; i < batchSize; i++ )
        {
            keeper.freeId( i );
        }

        // then
        // they should be returned in order
        for ( int i = 0; i < batchSize; i++ )
        {
            assertEquals( i, keeper.getId() );
        }
    }

    @Test
    public void shouldReadBackManyPersistedIdBatchesWhenAggressiveModeIsSet() throws Exception
    {
        // given
        StoreChannel channel = getStoreChannel();

        int batchSize = 10;
        FreeIdKeeper keeper = getFreeIdKeeperAggressive( channel, batchSize );
        Set<Long> freeIds = new HashSet<>();

        // when
        // we store enough ids to cause overflow to file, in two batches
        for ( long i = 0; i < batchSize * 2; i++ )
        {
            keeper.freeId( i );
            freeIds.add( i );
        }

        // then
        // they should be returned
        assertEquals( freeIds.size(), keeper.getCount() );
        for ( int i = batchSize * 2 - 1; i >= 0; i-- )
        {
            assertTrue( freeIds.remove( keeper.getId() ) );
        }
    }

    @Test
    public void shouldFirstReturnNonPersistedIdsAndThenPersistedOnesWhenAggressiveMode() throws Exception
    {
        // this is testing the stack property, but from the viewpoint of avoiding unnecessary disk reads
        // given
        StoreChannel channel = getStoreChannel();

        int batchSize = 10;
        FreeIdKeeper keeper = getFreeIdKeeperAggressive( channel, batchSize );

        // when
        // we store enough ids to cause overflow to file
        for ( int i = 0; i < batchSize; i++ )
        {
            keeper.freeId( i );
        }
        // and then some more
        int extraIds = 3;
        for ( int i = batchSize; i < batchSize + extraIds; i++ )
        {
            keeper.freeId( i );
        }

        // then
        // the first returned should be the newly freed ones
        for ( int i = batchSize; i < batchSize + extraIds; i++ )
        {
            assertEquals( i, keeper.getId() );
        }
        // and then there should be the persisted ones
        for ( int i = 0; i < batchSize; i++ )
        {
            assertEquals( i, keeper.getId() );
        }
    }

    @Test
    public void persistedIdsShouldStillBeCounted() throws Exception
    {
        // given
        StoreChannel channel = getStoreChannel();

        int batchSize = 10;
        FreeIdKeeper keeper = new FreeIdKeeper( channel, batchSize, true );

        // when
        // we store enough ids to cause overflow to file
        for ( int i = 0; i < batchSize; i++ )
        {
            keeper.freeId( i );
        }
        // and then some more
        int extraIds = 3;
        for ( int i = batchSize; i < batchSize + extraIds; i++ )
        {
            keeper.freeId( i );
        }

        // then
        // the count should be returned correctly
        assertEquals( batchSize + extraIds, keeper.getCount() );
    }

    @Test
    public void shouldStoreAndRestoreIds() throws Exception
    {
        // given
        StoreChannel channel = getStoreChannel();

        int batchSize = 10;
        FreeIdKeeper keeper = getFreeIdKeeperAggressive( channel, batchSize );
        Set<Long> freeIds = new HashSet<>(); // stack guarantees are not maintained between restarts

        // when
        // we store enough ids to cause overflow to file
        for ( long i = 0; i < batchSize; i++ )
        {
            keeper.freeId( i );
            freeIds.add( i );
        }
        // and then some more
        int extraIds = 3;
        for ( long i = batchSize; i < batchSize + extraIds; i++ )
        {
            keeper.freeId( i );
            freeIds.add( i );
        }
        // and then we close the keeper
        keeper.close();
        channel.close();
        // and then we open a new one over the same file
        channel = fs.get().open( new File( "id.file" ), OpenMode.READ_WRITE );
        keeper = getFreeIdKeeperAggressive( channel, batchSize );

        // then
        // the count should be returned correctly
        assertEquals( batchSize + extraIds, keeper.getCount() );
        assertEquals( freeIds.size(), keeper.getCount() );
        // and the ids, including the ones that did not cause a write, are still there (as a stack)
        for ( int i = batchSize + extraIds - 1; i >= 0; i-- )
        {
            long id = keeper.getId();
            assertTrue( freeIds.contains( id ) );
        }
    }

    @Test
    public void shouldNotReturnNewlyReleasedIdsIfAggressiveIsFalse() throws Exception
    {
        // given
        FreeIdKeeper keeper = getFreeIdKeeper();

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
        StoreChannel channel = spy( fs.get().open( new File( "id.file" ), OpenMode.READ_WRITE ) );

        int batchSize = 10;
        FreeIdKeeper keeper = getFreeIdKeeper( channel, batchSize );

        // when
        // enough ids are persisted to overflow
        for ( int i = 0; i < batchSize; i++ )
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
    public void shouldReturnIdsRestoredAndIgnoreNewlyReleasedIfAggressiveModeIsFalse() throws Exception
    {
        // given
        StoreChannel channel = getStoreChannel();

        int batchSize = 10;
        FreeIdKeeper keeper = getFreeIdKeeper( channel, batchSize );
        Set<Long> freeIds = new HashSet<>();
        for ( long i = 0; i < batchSize; i++ )
        {
            keeper.freeId( i );
            freeIds.add( i );
        }
        keeper.close();
        channel.close();
        // and then we open a new one over the same file
        channel = fs.get().open( new File( "id.file" ), OpenMode.READ_WRITE );
        keeper = getFreeIdKeeper( channel, batchSize );

        // when
        // we release some ids that spill to disk
        for ( int i = 0; i < batchSize; i++ )
        {
            keeper.freeId( i );
        }

        // then
        // we should retrieve all ids restored
        for ( int i = 0; i < batchSize; i++ )
        {
            assertTrue( freeIds.remove( keeper.getId() ) );
        }

        // then
        // we should have no ids to return
        assertEquals( NO_RESULT, keeper.getId() );
    }

    @Test
    public void shouldReturnNoResultIfIdsAreRestoredAndExhaustedAndThereAreFreeIdsFromThisRunWithAggressiveFalse() throws Exception
    {
        // given
        StoreChannel channel = getStoreChannel();

        int batchSize = 10;
        FreeIdKeeper keeper = getFreeIdKeeper( channel, batchSize );
        Set<Long> freeIds = new HashSet<>();
        for ( long i = 0; i < batchSize; i++ )
        {
            keeper.freeId( i );
            freeIds.add( i );
        }
        keeper.close();
        channel.close();
        // and then we open a new one over the same file
        channel = fs.get().open( new File( "id.file" ), OpenMode.READ_WRITE );
        keeper = getFreeIdKeeper( channel, batchSize );

        // when - then
        // we exhaust all ids restored
        for ( int i = 0; i < batchSize; i++ )
        {
            assertTrue( freeIds.remove( keeper.getId() ) );
        }

        // when
        // we release some ids that spill to disk
        for ( int i = 0; i < batchSize; i++ )
        {
            keeper.freeId( i );
        }

        // then
        // we should have no ids to return
        assertEquals( NO_RESULT, keeper.getId() );
    }

    @Test
    public void shouldNotReturnReusedIdsAfterRestart() throws Exception
    {
        // given
        StoreChannel channel = getStoreChannel();
        int batchSize = 10;
        FreeIdKeeper keeper = getFreeIdKeeperAggressive( channel, batchSize );
        long idGen = 0;

        // free 4 batches
        for ( long i = 0; i < batchSize * 4; i++ )
        {
            keeper.freeId( idGen++ );
        }

        // reuse 2 batches
        List<Long> reusedIds = new ArrayList<>();
        for ( int i = 0; i < batchSize * 2; i++ )
        {
            long id = keeper.getId();
            reusedIds.add( id );
        }

        // when
        keeper.close();
        channel.close();

        channel = getStoreChannel();
        keeper = getFreeIdKeeper( channel, batchSize );

        List<Long> remainingIds = new ArrayList<>();
        long id;
        while ( (id = keeper.getId()) != IdContainer.NO_RESULT )
        {
            remainingIds.add( id );
        }

        assertEquals( 2 * batchSize, remainingIds.size() );

        // then
        for ( Long remainingId : remainingIds )
        {
            assertFalse( reusedIds.contains( remainingId ) );
        }
    }

    @Test
    public void shouldTruncateFileInAggressiveMode() throws Exception
    {
        // given
        StoreChannel channel = getStoreChannel();
        int batchSize = 10;
        FreeIdKeeper keeper = getFreeIdKeeperAggressive( channel, batchSize );

        // free 4 batches
        for ( long i = 0; i < batchSize * 4; i++ )
        {
            keeper.freeId( i );
        }
        assertEquals( channel.size(), 4 * batchSize * Long.BYTES );

        // when
        for ( int i = 0; i < batchSize * 2; i++ )
        {
            keeper.getId();
        }

        // then
        assertEquals( channel.size(),  2 * batchSize * Long.BYTES );
    }

    @Test
    public void shouldCompactFileOnCloseInRegularMode() throws Exception
    {
        // given
        StoreChannel channel = getStoreChannel();
        int batchSize = 10;
        FreeIdKeeper keeper = getFreeIdKeeper( channel, batchSize );

        // free 4 batches
        for ( long i = 0; i < batchSize * 4; i++ )
        {
            keeper.freeId( i );
        }

        keeper.close();
        assertEquals( channel.size(), 4 * batchSize * Long.BYTES );
        channel.close();

        // after opening again the IDs should be free to reuse
        channel = getStoreChannel();
        keeper = getFreeIdKeeper( channel, batchSize );

        // free 4 more batches on top of the already existing 4
        for ( long i = 0; i < batchSize * 4; i++ )
        {
            keeper.freeId( i );
        }

        // fetch 2 batches
        for ( int i = 0; i < batchSize * 2; i++ )
        {
            keeper.getId();
        }

        keeper.close();

        // when
        assertEquals( channel.size(),  6 * batchSize * Long.BYTES );
    }

    @Test
    public void allocateEmptyBatchWhenNoIdsAreAvailable() throws IOException
    {
        FreeIdKeeper freeIdKeeper = getFreeIdKeeperAggressive();
        long[] ids = freeIdKeeper.getIds( 1024 );
        assertSame( PrimitiveLongCollections.EMPTY_LONG_ARRAY, ids );
        assertEquals( 0, freeIdKeeper.getCount() );
    }

    @Test
    public void allocateBatchWhenHaveMoreIdsInMemory() throws IOException
    {
        FreeIdKeeper freeIdKeeper = getFreeIdKeeperAggressive();
        for ( long id = 1L; id < 7L; id++ )
        {
            freeIdKeeper.freeId( id );
        }
        long[] ids = freeIdKeeper.getIds( 5 );
        assertArrayEquals( new long[]{1L, 2L, 3L, 4L, 5L}, ids);
        assertEquals( 1, freeIdKeeper.getCount() );
    }

    @Test
    public void allocateBatchWhenHaveLessIdsInMemory() throws IOException
    {
        FreeIdKeeper freeIdKeeper = getFreeIdKeeperAggressive();
        for ( long id = 1L; id < 4L; id++ )
        {
            freeIdKeeper.freeId( id );
        }
        long[] ids = freeIdKeeper.getIds( 5 );
        assertArrayEquals( new long[]{1L, 2L, 3L}, ids );
        assertEquals( 0, freeIdKeeper.getCount() );
    }

    @Test
    public void allocateBatchWhenHaveLessIdsInMemoryButHaveOnDiskMore() throws IOException
    {
        FreeIdKeeper freeIdKeeper = getFreeIdKeeperAggressive( 4 );
        for ( long id = 1L; id < 11L; id++ )
        {
            freeIdKeeper.freeId( id );
        }
        long[] ids = freeIdKeeper.getIds( 7 );
        assertArrayEquals( new long[]{9L, 10L, 5L, 6L, 7L, 8L, 1L}, ids );
        assertEquals( 3, freeIdKeeper.getCount() );
    }

    @Test
    public void allocateBatchWhenHaveLessIdsInMemoryAndOnDisk() throws IOException
    {
        FreeIdKeeper freeIdKeeper = getFreeIdKeeperAggressive( 4 );
        for ( long id = 1L; id < 10L; id++ )
        {
            freeIdKeeper.freeId( id );
        }
        long[] ids = freeIdKeeper.getIds( 15 );
        assertArrayEquals( new long[]{9L, 5L, 6L, 7L, 8L, 1L, 2L, 3L, 4L}, ids );
        assertEquals( 0, freeIdKeeper.getCount() );
    }

    private FreeIdKeeper getFreeIdKeeperAggressive() throws IOException
    {
        return getFreeIdKeeperAggressive( getStoreChannel(), 10 );
    }

    private FreeIdKeeper getFreeIdKeeperAggressive( int batchSize ) throws IOException
    {
        return getFreeIdKeeperAggressive( getStoreChannel(), batchSize );
    }

    private FreeIdKeeper getFreeIdKeeperAggressive( StoreChannel channel, int batchSize ) throws IOException
    {
        return getFreeIdKeeper( channel, batchSize, true );
    }

    private FreeIdKeeper getFreeIdKeeper( StoreChannel channel, int batchSize ) throws IOException
    {
        return getFreeIdKeeper( channel, batchSize, false );
    }

    private FreeIdKeeper getFreeIdKeeper() throws IOException
    {
        return getFreeIdKeeper( getStoreChannel(), 10 );
    }

    private FreeIdKeeper getFreeIdKeeper( StoreChannel channel, int batchSize, boolean aggressiveMode ) throws IOException
    {
        return new FreeIdKeeper( channel, batchSize, aggressiveMode );
    }

    private StoreChannel getStoreChannel() throws IOException
    {
        return fs.get().open( new File( "id.file" ), OpenMode.READ_WRITE );
    }
}
