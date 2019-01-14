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
package org.neo4j.csv.reader;

import org.junit.Test;

import java.io.IOException;
import org.neo4j.csv.reader.Source.Chunk;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import static java.util.Arrays.copyOfRange;

public class ClosestNewLineChunkerTest
{
    @Test
    public void shouldBackUpChunkToClosestNewline() throws Exception
    {
        // GIVEN
        CharReadable reader = Readables.wrap( "1234567\n8901234\n5678901234" );
        // (next chunks):                                   ^            ^
        // (actual chunks):                             ^        ^
        try ( ClosestNewLineChunker source = new ClosestNewLineChunker( reader, 12 ) )
        {
            // WHEN
            Chunk chunk = source.newChunk();
            assertTrue( source.nextChunk( chunk ) );
            assertArrayEquals( "1234567\n".toCharArray(), charactersOf( chunk ) );
            assertTrue( source.nextChunk( chunk ) );
            assertArrayEquals( "8901234\n".toCharArray(), charactersOf( chunk ) );
            assertTrue( source.nextChunk( chunk ) );
            assertArrayEquals( "5678901234".toCharArray(), charactersOf( chunk ) );

            // THEN
            assertFalse( source.nextChunk( chunk ) );
        }
    }

    @Test
    public void shouldFailIfNoNewlineInChunk() throws Exception
    {
        // GIVEN
        CharReadable reader = Readables.wrap( "1234567\n89012345678901234" );
        // (next chunks):                                   ^
        // (actual chunks):                             ^
        try ( ClosestNewLineChunker source = new ClosestNewLineChunker( reader, 12 ) )
        {
            // WHEN
            Chunk chunk = source.newChunk();
            assertTrue( source.nextChunk( chunk ) );
            assertArrayEquals( "1234567\n".toCharArray(), charactersOf( chunk ) );
            try
            {
                assertFalse( source.nextChunk( chunk ) );
                fail( "Should have failed here" );
            }
            catch ( IllegalStateException e )
            {
                // THEN good
            }
        }
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
            public int read( char[] into, int offset, int length )
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
            public SectionedCharBuffer read( SectionedCharBuffer buffer, int from )
            {
                throw new UnsupportedOperationException();
            }

            @Override
            public long length()
            {
                return 0;
            }
        };
    }

    static char[] charactersOf( Chunk chunk )
    {
        return copyOfRange( chunk.data(), chunk.startPosition(), chunk.startPosition() + chunk.length() );
    }
}
