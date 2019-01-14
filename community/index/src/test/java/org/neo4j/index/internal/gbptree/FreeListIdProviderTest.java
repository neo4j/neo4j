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
package org.neo4j.index.internal.gbptree;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

import org.neo4j.collection.primitive.Primitive;
import org.neo4j.collection.primitive.PrimitiveLongSet;
import org.neo4j.index.internal.gbptree.FreeListIdProvider.Monitor;
import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.io.pagecache.PagedFile;
import org.neo4j.test.rule.RandomRule;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.neo4j.index.internal.gbptree.FreeListIdProvider.NO_MONITOR;

public class FreeListIdProviderTest
{
    private static final int PAGE_SIZE = 128;
    private static final long GENERATION_ONE = GenerationSafePointer.MIN_GENERATION;
    private static final long GENERATION_TWO = GENERATION_ONE + 1;
    private static final long GENERATION_THREE = GENERATION_TWO + 1;
    private static final long GENERATION_FOUR = GENERATION_THREE + 1;
    private static final long BASE_ID = 5;

    private PageAwareByteArrayCursor cursor;
    private final PagedFile pagedFile = mock( PagedFile.class );
    private final FreelistPageMonitor monitor = new FreelistPageMonitor();
    private final FreeListIdProvider freelist = new FreeListIdProvider( pagedFile, PAGE_SIZE, BASE_ID, monitor );

    @Rule
    public final RandomRule random = new RandomRule();

    @Before
    public void setUpPagedFile() throws IOException
    {
        cursor = new PageAwareByteArrayCursor( PAGE_SIZE );
        when( pagedFile.io( anyLong(), anyInt() ) ).thenAnswer(
                invocation -> cursor.duplicate( invocation.getArgument( 0 ) ) );
        freelist.initialize( BASE_ID + 1, BASE_ID + 1, BASE_ID + 1, 0, 0 );
    }

    @Test
    public void shouldReleaseAndAcquireId() throws Exception
    {
        // GIVEN
        long releasedId = 11;
        fillPageWithRandomBytes( releasedId );

        // WHEN
        freelist.releaseId( GENERATION_ONE, GENERATION_TWO, releasedId );
        long acquiredId = freelist.acquireNewId( GENERATION_TWO, GENERATION_THREE );

        // THEN
        assertEquals( releasedId, acquiredId );
        cursor.next( acquiredId );
        assertEmpty( cursor );
    }

    @Test
    public void shouldReleaseAndAcquireIdsFromMultiplePages() throws Exception
    {
        // GIVEN
        int entries = freelist.entriesPerPage() + freelist.entriesPerPage() / 2;
        long baseId = 101;
        for ( int i = 0; i < entries; i++ )
        {
            freelist.releaseId( GENERATION_ONE, GENERATION_TWO, baseId + i );
        }

        // WHEN/THEN
        for ( int i = 0; i < entries; i++ )
        {
            long acquiredId = freelist.acquireNewId( GENERATION_TWO, GENERATION_THREE );
            assertEquals( baseId + i, acquiredId );
        }
    }

    @Test
    public void shouldPutFreedFreeListPagesIntoFreeListAsWell() throws Exception
    {
        // GIVEN
        long prevId;
        long acquiredId = BASE_ID + 1;
        long freelistPageId = BASE_ID + 1;
        PrimitiveLongSet released = Primitive.longSet();
        do
        {
            prevId = acquiredId;
            acquiredId = freelist.acquireNewId( GENERATION_ONE, GENERATION_TWO );
            freelist.releaseId( GENERATION_ONE, GENERATION_TWO, acquiredId );
            released.add( acquiredId );
        }
        while ( acquiredId - prevId == 1 );

        // WHEN
        while ( !released.isEmpty() )
        {
            long reAcquiredId = freelist.acquireNewId( GENERATION_TWO, GENERATION_THREE );
            released.remove( reAcquiredId );
        }

        // THEN
        assertEquals( freelistPageId, freelist.acquireNewId( GENERATION_THREE, GENERATION_FOUR ) );
    }

    @Test
    public void shouldStayBoundUnderStress() throws Exception
    {
        // GIVEN
        PrimitiveLongSet acquired = Primitive.longSet();
        List<Long> acquiredList = new ArrayList<>(); // for quickly finding random to remove
        long stableGeneration = GenerationSafePointer.MIN_GENERATION;
        long unstableGeneration = stableGeneration + 1;
        int iterations = 100;

        // WHEN
        for ( int i = 0; i < iterations; i++ )
        {
            for ( int j = 0; j < 10; j++ )
            {
                if ( random.nextBoolean() )
                {
                    // acquire
                    int count = random.intBetween( 5, 10 );
                    for ( int k = 0; k < count; k++ )
                    {
                        long acquiredId = freelist.acquireNewId( stableGeneration, unstableGeneration );
                        assertTrue( acquired.add( acquiredId ) );
                        acquiredList.add( acquiredId );
                    }
                }
                else
                {
                    // release
                    int count = random.intBetween( 5, 20 );
                    for ( int k = 0; k < count && !acquired.isEmpty(); k++ )
                    {
                        long id = acquiredList.remove( random.nextInt( acquiredList.size() ) );
                        assertTrue( acquired.remove( id ) );
                        freelist.releaseId( stableGeneration, unstableGeneration, id );
                    }
                }
            }

            for ( long id : acquiredList )
            {
                freelist.releaseId( stableGeneration, unstableGeneration, id );
            }
            acquiredList.clear();
            acquired.clear();

            // checkpoint, sort of
            stableGeneration = unstableGeneration;
            unstableGeneration++;
        }

        // THEN
        assertTrue( String.valueOf( freelist.lastId() ), freelist.lastId() < 200 );
    }

    @Test
    public void shouldVisitUnacquiredIds() throws Exception
    {
        // GIVEN a couple of released ids
        PrimitiveLongSet expected = Primitive.longSet();
        for ( int i = 0; i < 100; i++ )
        {
            expected.add( freelist.acquireNewId( GENERATION_ONE, GENERATION_TWO ) );
        }
        expected.visitKeys( id ->
        {
            freelist.releaseId( GENERATION_ONE, GENERATION_TWO, id );
            return false;
        } );
        // and only a few acquired
        for ( int i = 0; i < 10; i++ )
        {
            long acquiredId = freelist.acquireNewId( GENERATION_TWO, GENERATION_THREE );
            assertTrue( expected.remove( acquiredId ) );
        }

        // WHEN/THEN
        freelist.visitUnacquiredIds( unacquiredId -> assertTrue( expected.remove( unacquiredId ) ), GENERATION_THREE );
        assertTrue( expected.isEmpty() );
    }

    @Test
    public void shouldVisitFreelistPageIds() throws Exception
    {
        // GIVEN a couple of released ids
        PrimitiveLongSet expected = Primitive.longSet();
        // Add the already allocated free-list page id
        expected.add( BASE_ID + 1 );
        monitor.set( new Monitor()
        {
            @Override
            public void acquiredFreelistPageId( long freelistPageId )
            {
                expected.add( freelistPageId );
            }
        } );
        for ( int i = 0; i < 100; i++ )
        {
            long id = freelist.acquireNewId( GENERATION_ONE, GENERATION_TWO );
            freelist.releaseId( GENERATION_ONE, GENERATION_TWO, id );
        }
        assertTrue( expected.size() > 0 );

        // WHEN/THEN
        freelist.visitFreelistPageIds( id -> assertTrue( expected.remove( id ) ) );
        assertTrue( expected.isEmpty() );
    }

    private void fillPageWithRandomBytes( long releasedId )
    {
        cursor.next( releasedId );
        byte[] crapData = new byte[PAGE_SIZE];
        ThreadLocalRandom.current().nextBytes( crapData );
        cursor.putBytes( crapData );
    }

    private static void assertEmpty( PageCursor cursor )
    {
        byte[] data = new byte[PAGE_SIZE];
        cursor.getBytes( data );
        for ( byte b : data )
        {
            assertEquals( 0, b );
        }
    }

    private static class FreelistPageMonitor implements Monitor
    {
        private Monitor actual = NO_MONITOR;

        void set( Monitor actual )
        {
            this.actual = actual;
        }

        @Override
        public void acquiredFreelistPageId( long freelistPageId )
        {
            actual.acquiredFreelistPageId( freelistPageId );
        }

        @Override
        public void releasedFreelistPageId( long freelistPageId )
        {
            actual.releasedFreelistPageId( freelistPageId );
        }
    }
}
