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

import java.io.StringReader;

import org.neo4j.csv.reader.Source.Chunk;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import static org.neo4j.csv.reader.ClosestNewLineChunkerTest.charactersOf;

public class MultiLineAwareChunkerTest
{
    @Test
    public void shouldBackUpChunkToClosestNewline() throws Exception
    {
        // GIVEN
        CharReadable reader = Readables.wrap( new StringReader(
                "1,2,3,4,5\n" +
                "6,7,8,9,10"  ) );
        try ( Chunker source = new MultiLineAwareChunker( reader, config( 15 ), 5, ',' ) )
        {
            // WHEN
            Chunk chunk = source.newChunk();
            assertTrue( source.nextChunk( chunk ) );
            assertArrayEquals( "1,2,3,4,5\n".toCharArray(), charactersOf( chunk ) );
            assertTrue( source.nextChunk( chunk ) );
            assertArrayEquals( "6,7,8,9,10".toCharArray(), charactersOf( chunk ) );

            // THEN
            assertFalse( source.nextChunk( chunk ) );
        }
    }

    private static Configuration config( int bufferSize )
    {
        return new Configuration.Default()
        {
            @Override
            public int bufferSize()
            {
                return bufferSize;
            }
        };
    }
}
