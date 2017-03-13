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
package org.neo4j.index.internal.gbptree;

import org.junit.Test;

import static org.junit.Assert.fail;

import static org.neo4j.index.internal.gbptree.GenSafePointer.MIN_GEN;
import static org.neo4j.index.internal.gbptree.GenSafePointerPair.MAX_GEN_OFFSET_MASK;
import static org.neo4j.index.internal.gbptree.GenSafePointerPair.read;
import static org.neo4j.index.internal.gbptree.GenSafePointerPair.write;
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
        long firstGen = MIN_GEN;
        long secondGen = firstGen + 1;
        long thirdGen = secondGen + 1;
        int offset = 0;
        cursor.setOffset( offset );
        write( cursor, 10, firstGen, secondGen );
        cursor.setOffset( offset );
        write( cursor, 11, secondGen, thirdGen );

        try
        {
            // WHEN
            cursor.setOffset( offset );
            read( cursor, secondGen, thirdGen, (int) (MAX_GEN_OFFSET_MASK + 1) );
            fail( "Should have failed" );
        }
        catch ( IllegalArgumentException e )
        {
            // THEN good
        }
    }
}
