/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
package org.neo4j.csv.reader;

import org.junit.Test;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicInteger;

import org.neo4j.csv.reader.Source.Chunk;

import static java.util.Arrays.copyOfRange;
import static java.util.concurrent.Executors.newFixedThreadPool;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.neo4j.csv.reader.Source.EMPTY_CHUNK;

public class ProcessingSourceTest
{
    @Test
    public void shouldBackUpChunkToClosestNewline() throws Exception
    {
        // GIVEN
        CharReadable reader = Readables.wrap( "1234567\n8901234\n5678901234" );
        // (next chunks):                                   ^            ^
        // (actual chunks):                             ^        ^
        try ( ProcessingSource source = new ProcessingSource( reader, 12, 1 ) )
        {
            // WHEN
            Chunk first = source.nextChunk();
            assertArrayEquals( "1234567\n".toCharArray(), charactersOf( first ) );
            Chunk second = source.nextChunk();
            assertArrayEquals( "8901234\n".toCharArray(), charactersOf( second ) );
            Chunk third = source.nextChunk();
            assertArrayEquals( "5678901234".toCharArray(), charactersOf( third ) );

            // THEN
            assertEquals( 0, source.nextChunk().length() );
        }
    }

    @Test
    public void shouldFailIfNoNewlineInChunk() throws Exception
    {
        // GIVEN
        CharReadable reader = Readables.wrap( "1234567\n89012345678901234" );
        // (next chunks):                                   ^
        // (actual chunks):                             ^
        try ( ProcessingSource source = new ProcessingSource( reader, 12, 1 ) )
        {
            // WHEN
            Chunk first = source.nextChunk();
            assertArrayEquals( "1234567\n".toCharArray(), charactersOf( first ) );
            try
            {
                source.nextChunk();
                fail( "Should have failed here" );
            }
            catch ( IllegalStateException e )
            {
                // THEN good
            }
        }
    }

    @Test
    public void shouldReuseBuffers() throws Exception
    {
        // GIVEN
        ProcessingSource source = new ProcessingSource( dataWithLines( 2 ), 100, 1 );

        // WHEN
        Chunk firstChunk = source.nextChunk();
        char[] firstBuffer = firstChunk.data();
        firstChunk.close();

        // THEN
        Chunk secondChunk = source.nextChunk();
        char[] secondBuffer = secondChunk.data();
        secondChunk.close();
        assertSame( firstBuffer, secondBuffer );
        source.close();
    }

    @Test
    public void shouldReuseBuffersEventually() throws Exception
    {
        // GIVEN
        ProcessingSource source = new ProcessingSource( dataWithLines( 5 ), 100, 2 );
        Chunk firstChunk = source.nextChunk();
        char[] firstBuffer = firstChunk.data();

        // WHEN
        Chunk secondChunk = source.nextChunk();
        char[] secondBuffer = secondChunk.data();
        assertNotSame( secondBuffer, firstBuffer );

        // THEN
        firstChunk.close();
        Chunk thirdChunk = source.nextChunk();
        char[] thirdBuffer = thirdChunk.data();
        assertSame( firstBuffer, thirdBuffer );

        secondChunk.close();
        thirdChunk.close();
        source.close();
    }

    @Test
    public void shouldStressReuse() throws Exception
    {
        // GIVEN
        int nThreads = 10;
        ProcessingSource source = new ProcessingSource( dataWithLines( 3_000 ), 100, nThreads );
        ExecutorService executor = newFixedThreadPool( nThreads );
        AtomicInteger activeProcessors = new AtomicInteger();

        // WHEN
        Chunk chunk = EMPTY_CHUNK;
        Set<char[]> observedDataArrays = new HashSet<>();
        do
        {
            while ( activeProcessors.get() == nThreads )
            {   // Provide push-back which normally happens when using a ProcessingSource, although perhaps not
            }   // with a busy-wait like this, but that's not really important.

            // Get next chunk and register the array instance we got
            chunk = source.nextChunk();
            observedDataArrays.add( chunk.data() );

            // Update data for push-back of the load in this test
            activeProcessors.incrementAndGet();

            // Submit this chunk for processing (no-op) and closing (reuse)
            Chunk currentChunk = chunk;
            executor.submit( () ->
            {
                currentChunk.close();
                activeProcessors.decrementAndGet();
            } );
        }
        while ( chunk.length() > 0 );
        executor.shutdown();
        executor.awaitTermination( 100, SECONDS );

        // THEN
        source.close();
        assertTrue( "" + observedDataArrays.size(),
                observedDataArrays.size() >= 1 && observedDataArrays.size() <= nThreads );
    }

    private CharReadable dataWithLines( int lineCount )
    {
        return new CharReadable.Adapter()
        {
            private int line;

            @Override
            public String sourceDescription()
            {
                return "test";
            }

            @Override
            public int read( char[] into, int offset, int length ) throws IOException
            {
                assert offset == 0 : "This test assumes offset is 0, "
                        + "which it always was for this use case at the time of writing";
                if ( line++ == lineCount )
                {
                    return -1;
                }

                // We cheat here and simply say that we read the requested amount of characters
                into[length - 1] = '\n';
                return length;
            }

            @Override
            public SectionedCharBuffer read( SectionedCharBuffer buffer, int from ) throws IOException
            {
                throw new UnsupportedOperationException();
            }

            @Override
            public long length()
            {
                return lineCount * 10;
            }
        };
    }

    private char[] charactersOf( Chunk chunk )
    {
        return copyOfRange( chunk.data(), chunk.startPosition(), chunk.startPosition() + chunk.length() );
    }
}
