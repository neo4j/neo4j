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

import org.junit.Test;

import org.neo4j.io.pagecache.PageCache;

import static org.junit.Assert.fail;
import static org.neo4j.index.internal.gbptree.GenerationSafePointer.MIN_GENERATION;
import static org.neo4j.index.internal.gbptree.GenerationSafePointerPair.MAX_GENERATION_OFFSET_MASK;
import static org.neo4j.index.internal.gbptree.GenerationSafePointerPair.read;
import static org.neo4j.index.internal.gbptree.GenerationSafePointerPair.write;

public class GenerationSafePointerPairAdditionalTest
{
    @Test
    public void shouldFailFastOnTooLargeGenerationOffset()
    {
        // GIVEN
        int pageSize = PageCache.PAGE_SIZE;
        PageAwareByteArrayCursor cursor = new PageAwareByteArrayCursor( pageSize );
        cursor.next( 0 );
        long firstGeneration = MIN_GENERATION;
        long secondGeneration = firstGeneration + 1;
        long thirdGeneration = secondGeneration + 1;
        int offset = 0;
        cursor.setOffset( offset );
        write( cursor, 10, firstGeneration, secondGeneration );
        cursor.setOffset( offset );
        write( cursor, 11, secondGeneration, thirdGeneration );

        try
        {
            // WHEN
            cursor.setOffset( offset );
            read( cursor, secondGeneration, thirdGeneration, (int) (MAX_GENERATION_OFFSET_MASK + 1) );
            fail( "Should have failed" );
        }
        catch ( IllegalArgumentException e )
        {
            // THEN good
        }
    }
}
