/*
 * Copyright (c) 2002-2020 "Neo4j,"
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

import org.junit.jupiter.api.Test;

import org.neo4j.csv.reader.Source.Chunk;

import static java.util.Arrays.copyOfRange;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ClosestNewLineChunkerTest
{
    @Test
    void shouldBackUpChunkToClosestNewline() throws Exception
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
    void shouldFailIfNoNewlineInChunk() throws Exception
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
            assertThrows( IllegalStateException.class, () -> assertFalse( source.nextChunk( chunk ) ) );
        }
    }

    private static char[] charactersOf( Chunk chunk )
    {
        return copyOfRange( chunk.data(), chunk.startPosition(), chunk.startPosition() + chunk.length() );
    }
}
