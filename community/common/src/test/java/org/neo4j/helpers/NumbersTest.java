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
package org.neo4j.helpers;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import static org.junit.Assert.assertEquals;
import static org.neo4j.helpers.Numbers.safeCastIntToShort;
import static org.neo4j.helpers.Numbers.safeCastIntToUnsignedShort;
import static org.neo4j.helpers.Numbers.safeCastLongToByte;
import static org.neo4j.helpers.Numbers.safeCastLongToInt;
import static org.neo4j.helpers.Numbers.safeCastLongToShort;
import static org.neo4j.helpers.Numbers.unsignedShortToInt;

public class NumbersTest
{

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    @Test
    public void failSafeCastLongToInt()
    {
        expectedException.expect( ArithmeticException.class );
        expectedException.expectMessage( "Value 2147483648 is too big to be represented as int" );

        safeCastLongToInt( Integer.MAX_VALUE + 1L );
    }

    @Test
    public void failSafeCastLongToShort()
    {
        expectedException.expect( ArithmeticException.class );
        expectedException.expectMessage( "Value 32768 is too big to be represented as short" );

        safeCastLongToShort( Short.MAX_VALUE + 1L );
    }

    @Test
    public void failSafeCastIntToUnsignedShort()
    {
        expectedException.expect( ArithmeticException.class );
        expectedException.expectMessage( "Value 131068 is too big to be represented as unsigned short" );

        safeCastIntToUnsignedShort( Short.MAX_VALUE << 2 );
    }

    @Test
    public void failSafeCastLongToByte()
    {
        expectedException.expect( ArithmeticException.class );
        expectedException.expectMessage( "Value 128 is too big to be represented as byte" );

        safeCastLongToByte( Byte.MAX_VALUE + 1 );
    }

    @Test
    public void failSafeCastIntToShort()
    {
        expectedException.expect( ArithmeticException.class );
        expectedException.expectMessage( "Value 32768 is too big to be represented as short" );

        safeCastIntToShort( Short.MAX_VALUE + 1 );
    }

    @Test
    public void castLongToInt()
    {
        assertEquals(1, safeCastLongToInt( 1L ));
        assertEquals(10, safeCastLongToInt( 10L ));
        assertEquals(-1, safeCastLongToInt( -1L ));
        assertEquals(Integer.MAX_VALUE, safeCastLongToInt( Integer.MAX_VALUE ));
        assertEquals(Integer.MIN_VALUE, safeCastLongToInt( Integer.MIN_VALUE ));
    }

    @Test
    public void castLongToShort()
    {
        assertEquals(1, safeCastLongToShort( 1L ));
        assertEquals(10, safeCastLongToShort( 10L ));
        assertEquals(-1, safeCastLongToShort( -1L ));
        assertEquals(Short.MAX_VALUE, safeCastLongToShort( Short.MAX_VALUE ));
        assertEquals(Short.MIN_VALUE, safeCastLongToShort( Short.MIN_VALUE ));
    }

    @Test
    public void castIntToUnsighedShort()
    {
        assertEquals(1, safeCastIntToUnsignedShort( 1 ));
        assertEquals(10, safeCastIntToUnsignedShort( 10 ));
        assertEquals( -1, safeCastIntToUnsignedShort( (Short.MAX_VALUE << 1) + 1 ) );
    }

    @Test
    public void castIntToShort()
    {
        assertEquals(1, safeCastIntToShort( 1 ));
        assertEquals(10, safeCastIntToShort( 10 ));
        assertEquals( Short.MAX_VALUE, safeCastIntToShort( Short.MAX_VALUE ) );
        assertEquals( Short.MIN_VALUE, safeCastIntToShort( Short.MIN_VALUE ) );
    }

    @Test
    public void castLongToByte()
    {
        assertEquals(1, safeCastLongToByte( 1L ));
        assertEquals(10, safeCastLongToByte( 10L ));
        assertEquals(-1, safeCastLongToByte( -1L ));
        assertEquals(Byte.MAX_VALUE, safeCastLongToByte( Byte.MAX_VALUE ));
        assertEquals(Byte.MIN_VALUE, safeCastLongToByte( Byte.MIN_VALUE ));
    }

    @Test
    public void castUnsignedShortToInt()
    {
        assertEquals( 1, unsignedShortToInt( (short) 1 ) );
        assertEquals( Short.MAX_VALUE, unsignedShortToInt( Short.MAX_VALUE ) );
        assertEquals( (Short.MAX_VALUE << 1) | 1, unsignedShortToInt( (short) -1 ) );
    }
}
