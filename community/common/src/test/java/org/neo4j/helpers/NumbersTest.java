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


import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.helpers.Numbers.ceilingPowerOfTwo;
import static org.neo4j.helpers.Numbers.isPowerOfTwo;
import static org.neo4j.helpers.Numbers.safeCastIntToShort;
import static org.neo4j.helpers.Numbers.safeCastIntToUnsignedShort;
import static org.neo4j.helpers.Numbers.safeCastLongToByte;
import static org.neo4j.helpers.Numbers.safeCastLongToInt;
import static org.neo4j.helpers.Numbers.safeCastLongToShort;
import static org.neo4j.helpers.Numbers.unsignedShortToInt;

class NumbersTest
{
    @Test
    void failSafeCastLongToInt()
    {
        ArithmeticException exception = assertThrows( ArithmeticException.class, () -> safeCastLongToInt( Integer.MAX_VALUE + 1L ) );
        assertEquals( "Value 2147483648 is too big to be represented as int", exception.getMessage() );
    }

    @Test
    void failSafeCastLongToShort()
    {
        ArithmeticException exception = assertThrows( ArithmeticException.class, () -> safeCastLongToShort( Short.MAX_VALUE + 1L ) );
        assertEquals( "Value 32768 is too big to be represented as short", exception.getMessage() );
    }

    @Test
    void failSafeCastIntToUnsignedShort()
    {
        ArithmeticException exception = assertThrows( ArithmeticException.class, () -> safeCastIntToUnsignedShort( Short.MAX_VALUE << 2 ) );
        assertEquals( "Value 131068 is too big to be represented as unsigned short", exception.getMessage() );
    }

    @Test
    void failSafeCastLongToByte()
    {
        ArithmeticException exception = assertThrows( ArithmeticException.class, () -> safeCastLongToByte( Byte.MAX_VALUE + 1 ) );
        assertEquals( "Value 128 is too big to be represented as byte", exception.getMessage() );
    }

    @Test
    void failSafeCastIntToShort()
    {
        ArithmeticException exception = assertThrows( ArithmeticException.class, () -> safeCastIntToShort( Short.MAX_VALUE + 1 ) );
        assertEquals( "Value 32768 is too big to be represented as short", exception.getMessage() );
    }

    @Test
    void castLongToInt()
    {
        assertEquals(1, safeCastLongToInt( 1L ));
        assertEquals(10, safeCastLongToInt( 10L ));
        assertEquals(-1, safeCastLongToInt( -1L ));
        assertEquals(Integer.MAX_VALUE, safeCastLongToInt( Integer.MAX_VALUE ));
        assertEquals(Integer.MIN_VALUE, safeCastLongToInt( Integer.MIN_VALUE ));
    }

    @Test
    void castLongToShort()
    {
        assertEquals(1, safeCastLongToShort( 1L ));
        assertEquals(10, safeCastLongToShort( 10L ));
        assertEquals(-1, safeCastLongToShort( -1L ));
        assertEquals(Short.MAX_VALUE, safeCastLongToShort( Short.MAX_VALUE ));
        assertEquals(Short.MIN_VALUE, safeCastLongToShort( Short.MIN_VALUE ));
    }

    @Test
    void castIntToUnsignedShort()
    {
        assertEquals(1, safeCastIntToUnsignedShort( 1 ));
        assertEquals(10, safeCastIntToUnsignedShort( 10 ));
        assertEquals( -1, safeCastIntToUnsignedShort( (Short.MAX_VALUE << 1) + 1 ) );
    }

    @Test
    void castIntToShort()
    {
        assertEquals(1, safeCastIntToShort( 1 ));
        assertEquals(10, safeCastIntToShort( 10 ));
        assertEquals( Short.MAX_VALUE, safeCastIntToShort( Short.MAX_VALUE ) );
        assertEquals( Short.MIN_VALUE, safeCastIntToShort( Short.MIN_VALUE ) );
    }

    @Test
    void castLongToByte()
    {
        assertEquals(1, safeCastLongToByte( 1L ));
        assertEquals(10, safeCastLongToByte( 10L ));
        assertEquals(-1, safeCastLongToByte( -1L ));
        assertEquals(Byte.MAX_VALUE, safeCastLongToByte( Byte.MAX_VALUE ));
        assertEquals(Byte.MIN_VALUE, safeCastLongToByte( Byte.MIN_VALUE ));
    }

    @Test
    void castUnsignedShortToInt()
    {
        assertEquals( 1, unsignedShortToInt( (short) 1 ) );
        assertEquals( Short.MAX_VALUE, unsignedShortToInt( Short.MAX_VALUE ) );
        assertEquals( (Short.MAX_VALUE << 1) | 1, unsignedShortToInt( (short) -1 ) );
    }

    @Test
    void checkLongCeilingPowerOfTwo()
    {
        assertEquals( 1L, ceilingPowerOfTwo( 1L ) );
        assertEquals( 2L, ceilingPowerOfTwo( 2L ) );
        assertEquals( 8L, ceilingPowerOfTwo( 5L ) );
        assertEquals( 32L, ceilingPowerOfTwo( 32L ) );
        assertEquals( 1024L, ceilingPowerOfTwo( 1023L ) );
        assertThrows( IllegalArgumentException.class, () -> ceilingPowerOfTwo( Long.MAX_VALUE ) );
        assertThrows( IllegalArgumentException.class, () -> ceilingPowerOfTwo( 0L ) );
        assertThrows( IllegalArgumentException.class, () -> ceilingPowerOfTwo( -1L ) );
        assertThrows( IllegalArgumentException.class, () -> ceilingPowerOfTwo( Long.MIN_VALUE ) );
    }

    @Test
    void checkIntCeilingPowerOfTwo()
    {
        assertEquals( 1, ceilingPowerOfTwo( 1 ) );
        assertEquals( 2, ceilingPowerOfTwo( 2 ) );
        assertEquals( 8, ceilingPowerOfTwo( 5 ) );
        assertEquals( 32, ceilingPowerOfTwo( 32 ) );
        assertEquals( 1024, ceilingPowerOfTwo( 1023 ) );
        assertThrows( IllegalArgumentException.class, () -> ceilingPowerOfTwo( Integer.MAX_VALUE ) );
        assertThrows( IllegalArgumentException.class, () -> ceilingPowerOfTwo( 0 ) );
        assertThrows( IllegalArgumentException.class, () -> ceilingPowerOfTwo( -1 ) );
        assertThrows( IllegalArgumentException.class, () -> ceilingPowerOfTwo( Integer.MIN_VALUE ) );
    }

    @Test
    void checkPowerOfTwo()
    {
        assertTrue( isPowerOfTwo( 1 ) );
        assertFalse( isPowerOfTwo( 5 ) );
        assertTrue( isPowerOfTwo( 8 ) );
        assertTrue( isPowerOfTwo( 1024 ) );
        assertFalse( isPowerOfTwo( Long.MAX_VALUE ) );
        assertThrows( IllegalArgumentException.class, () -> isPowerOfTwo( 0 ) );
        assertThrows( IllegalArgumentException.class, () -> isPowerOfTwo( -1 ) );
        assertThrows( IllegalArgumentException.class, () -> isPowerOfTwo( Long.MIN_VALUE ) );
    }
}
