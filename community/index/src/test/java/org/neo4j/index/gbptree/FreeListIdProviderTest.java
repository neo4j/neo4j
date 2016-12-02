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
package org.neo4j.index.gbptree;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

import org.neo4j.collection.primitive.Primitive;
import org.neo4j.collection.primitive.PrimitiveLongSet;
import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.io.pagecache.PagedFile;
import org.neo4j.test.rule.RandomRule;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class FreeListIdProviderTest
{
    private static final int PAGE_SIZE = 128;
    private static final long GENERATION_ONE = GenSafePointer.MIN_GENERATION;
    private static final long GENERATION_TWO = GENERATION_ONE + 1;
    private static final long GENERATION_THREE = GENERATION_TWO + 1;
    private static final long GENERATION_FOUR = GENERATION_THREE + 1;
    private static final long BASE_ID = 5;

    private final PageAwareByteArrayCursor cursor = new PageAwareByteArrayCursor( PAGE_SIZE );
    private final PagedFile pagedFile = mock( PagedFile.class );
    private final FreeListIdProvider freelist = new FreeListIdProvider( pagedFile, PAGE_SIZE, BASE_ID );

    @Rule
    public final RandomRule random = new RandomRule();

    @Before
    public void setUpPagedFile() throws IOException
    {
        when( pagedFile.io( anyLong(), anyInt() ) ).thenAnswer(
                invocation -> cursor.duplicate( invocation.getArgumentAt( 0, Long.class ).longValue() ) );
        freelist.initialize( BASE_ID + 1, BASE_ID + 1, BASE_ID + 1, 0, 0 );
    }

    @Test
    public void shouldReleaseAndAcquireId() throws Exception
    {
        // GIVEN
        long releasedId = 11;
        fillPageWithCrapData( releasedId );

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
        long stableGeneration = GenSafePointer.MIN_GENERATION;
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

    private void fillPageWithCrapData( long releasedId ) throws IOException
    {
        cursor.next( releasedId );
        byte[] crapData = new byte[PAGE_SIZE];
        ThreadLocalRandom.current().nextBytes( crapData );
        cursor.putBytes( crapData );
    }

    private void assertEmpty( PageCursor cursor )
    {
        byte[] data = new byte[PAGE_SIZE];
        cursor.getBytes( data );
        for ( byte b : data )
        {
            assertEquals( 0, b );
        }
    }
}
