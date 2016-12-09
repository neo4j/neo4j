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

import org.junit.Test;

import static org.junit.Assert.fail;

import static org.neo4j.index.gbptree.GenSafePointer.MIN_GENERATION;
import static org.neo4j.index.gbptree.GenSafePointerPair.MAX_GEN_OFFSET_MASK;
import static org.neo4j.index.gbptree.GenSafePointerPair.read;
import static org.neo4j.index.gbptree.GenSafePointerPair.write;
import static org.neo4j.io.ByteUnit.kibiBytes;

public class GenSafePointerPairAdditionalTest
{
    @Test
    public void shouldFailFastOnTooLargeGenOffset() throws Exception
    {
        // GIVEN
        int pageSize = (int) kibiBytes( 8 );
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
            read( cursor, secondGeneration, thirdGeneration, (int) (MAX_GEN_OFFSET_MASK + 1) );
            fail( "Should have failed" );
        }
        catch ( IllegalArgumentException e )
        {
            // THEN good
        }
    }
}
