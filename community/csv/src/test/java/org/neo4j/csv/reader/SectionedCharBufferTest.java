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
package org.neo4j.csv.reader;

import org.junit.Test;

import java.io.Reader;
import java.io.StringReader;

import static org.junit.Assert.assertEquals;

public class SectionedCharBufferTest
{
    @Test
    public void shouldCompactIntoItself() throws Exception
    {
        // GIVEN
        Reader data = new StringReader( "01234567" );
        SectionedCharBuffer buffer = new SectionedCharBuffer( 4 ); // will yield an 8-char array in total
        buffer.readFrom( data );

        // WHEN
        buffer.compact( buffer, buffer.front()-2 );

        // THEN
        assertEquals( '2', buffer.array()[2] );
        assertEquals( '3', buffer.array()[3] );
    }

    @Test
    public void shouldCompactIntoAnotherBuffer() throws Exception
    {
        // GIVEN
        Reader data = new StringReader( "01234567" );
        SectionedCharBuffer buffer1 = new SectionedCharBuffer( 8 );
        SectionedCharBuffer buffer2 = new SectionedCharBuffer( 8 );
        buffer1.readFrom( data );

        // WHEN
        buffer2.readFrom( data );
        // simulate reading 2 chars as one value, then reading 2 bytes and requesting more
        buffer1.compact( buffer2, buffer1.pivot()+2 /*simulate reading 2 chars*/ );

        // THEN
        assertEquals( '2', buffer2.array()[2] );
        assertEquals( '3', buffer2.array()[3] );
        assertEquals( '4', buffer2.array()[4] );
        assertEquals( '5', buffer2.array()[5] );
        assertEquals( '6', buffer2.array()[6] );
        assertEquals( '7', buffer2.array()[7] );
    }
}
