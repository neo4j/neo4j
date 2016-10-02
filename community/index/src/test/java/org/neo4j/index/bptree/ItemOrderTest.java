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
package org.neo4j.index.bptree;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class ItemOrderTest
{
    @Test
    public void shouldInterpretOrderCorrectly() throws Exception
    {
        // GIVEN
        int count = 5;
        // This basically means:
        // Insert A at pos:0 when length:0 --> 1 | [A]
        // Insert B at pos:1 when length:1 --> 2 | [A,B]
        // Insert C at pos:0 when length:2 --> 3 | [C,A,B]
        // Insert D at pos:0 when length:3 --> 4 | [D,C,A,B]
        // Insert E at pos:1 when length:4 --> 5 | [D,E,C,A,B]
        byte[] jumpList = new byte[] {0, 1, 0, 0, 1};

        //                                                 counts[0,0,0,0,0]
        //                                        1  -->   counts[0,1,0,0,0]
        //                                     0     -->   counts[1,1,0,0,0]
        //                                  2        -->   counts[2,1,0,0,0]
        //                               4           -->   counts[2,2,0,0,0]
        //                            3              -->   counts[2,2,0,0,0]

        ItemOrder order = new ItemOrder( count );

        // WHEN
        order.read( ByteArrayPageCursor.wrap( jumpList, 0, count ), count );

        // THEN order should be:
        assertEquals( "A", 3, order.physicalPosition( 0 ) );
        assertEquals( "B", 4, order.physicalPosition( 1 ) );
        assertEquals( "C", 2, order.physicalPosition( 2 ) );
        assertEquals( "D", 0, order.physicalPosition( 3 ) );
        assertEquals( "E", 1, order.physicalPosition( 4 ) );
    }

    @Test
    public void shouldHandleFullUnsignedByte() throws Exception
    {
        // GIVEN
        int count = 0x100;
        ItemOrder order = new ItemOrder( count );
        byte[] jumpList = new byte[count];
        for ( int i = 0; i < count; i++ )
        {
            jumpList[i] = (byte) i;
        }

        // WHEN
        order.read( ByteArrayPageCursor.wrap( jumpList, 0, count ), count );

        // THEN
        for ( int i = 0; i < count; i++ )
        {
            assertEquals( i, order.physicalPosition( i ) );
        }
    }
}
