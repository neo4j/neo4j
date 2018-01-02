/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.unsafe.impl.batchimport.cache.idmapping.string;

import org.junit.Test;

import org.neo4j.collection.primitive.Primitive;
import org.neo4j.collection.primitive.PrimitiveLongSet;

import static org.junit.Assert.assertTrue;

public class StringEncoderTest
{
    @Test
    public void shouldEncodeStringWithZeroLength() throws Exception
    {
        // GIVEN
        Encoder encoder = new StringEncoder();

        // WHEN
        long eId = encoder.encode( "" );

        // THEN
        assertTrue( eId != 0 );
    }

    @Test
    public void shouldEncodeStringWithAnyLength() throws Exception
    {
        // GIVEN
        Encoder encoder = new StringEncoder();

        // WHEN
        PrimitiveLongSet encoded = Primitive.longSet();
        int total = 1_000, duplicates = 0;
        for ( int i = 0; i < total; i++ )
        {
            // THEN
            long encode = encoder.encode( abcStringOfLength( i ) );
            assertTrue( encode != 0 );
            if ( !encoded.add( encode ) )
            {
                duplicates++;
            }
        }
        assertTrue( ((float) duplicates / (float) total) < 0.01f );
    }

    private String abcStringOfLength( int length )
    {
        char[] chars = new char[length];
        for ( int i = 0; i < length; i++ )
        {
            int ch = 'a' + (i%20);
            chars[i] = (char) ch;
        }
        return new String( chars );
    }
}
