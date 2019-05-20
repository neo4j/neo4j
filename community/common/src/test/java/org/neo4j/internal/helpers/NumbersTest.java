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
package org.neo4j.internal.helpers;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class NumbersTest
{
    @Test
    void failSafeCastLongToInt()
    {
        ArithmeticException exception = assertThrows( ArithmeticException.class, () -> Numbers.safeCastLongToInt( Integer.MAX_VALUE + 1L ) );
        assertEquals( "Value 2147483648 is too big to be represented as int", exception.getMessage() );
    }

    @Test
    void failSafeCastLongToShort()
    {
        ArithmeticException exception = assertThrows( ArithmeticException.class, () -> Numbers.safeCastLongToShort( Short.MAX_VALUE + 1L ) );
        assertEquals( "Value 32768 is too big to be represented as short", exception.getMessage() );
    }

    @Test
    void failSafeCastIntToUnsignedShort()
    {
        ArithmeticException exception = assertThrows( ArithmeticException.class, () -> Numbers.safeCastIntToUnsignedShort( Short.MAX_VALUE << 2 ) );
        assertEquals( "Value 131068 is too big to be represented as unsigned short", exception.getMessage() );
    }

    @Test
    void failSafeCastLongToByte()
    {
        ArithmeticException exception = assertThrows( ArithmeticException.class, () -> Numbers.safeCastLongToByte( Byte.MAX_VALUE + 1 ) );
        assertEquals( "Value 128 is too big to be represented as byte", exception.getMessage() );
    }

    @Test
    void failSafeCastIntToShort()
    {
        ArithmeticException exception = assertThrows( ArithmeticException.class, () -> Numbers.safeCastIntToShort( Short.MAX_VALUE + 1 ) );
        assertEquals( "Value 32768 is too big to be represented as short", exception.getMessage() );
    }

    @Test
    void castLongToInt()
    {
        Assertions.assertEquals(1, Numbers.safeCastLongToInt( 1L ));
        Assertions.assertEquals(10, Numbers.safeCastLongToInt( 10L ));
        Assertions.assertEquals(-1, Numbers.safeCastLongToInt( -1L ));
        Assertions.assertEquals(Integer.MAX_VALUE, Numbers.safeCastLongToInt( Integer.MAX_VALUE ));
        Assertions.assertEquals(Integer.MIN_VALUE, Numbers.safeCastLongToInt( Integer.MIN_VALUE ));
    }

    @Test
    void castLongToShort()
    {
        Assertions.assertEquals(1, Numbers.safeCastLongToShort( 1L ));
        Assertions.assertEquals(10, Numbers.safeCastLongToShort( 10L ));
        Assertions.assertEquals(-1, Numbers.safeCastLongToShort( -1L ));
        Assertions.assertEquals(Short.MAX_VALUE, Numbers.safeCastLongToShort( Short.MAX_VALUE ));
        Assertions.assertEquals(Short.MIN_VALUE, Numbers.safeCastLongToShort( Short.MIN_VALUE ));
    }

    @Test
    void castIntToUnsignedShort()
    {
        Assertions.assertEquals(1, Numbers.safeCastIntToUnsignedShort( 1 ));
        Assertions.assertEquals(10, Numbers.safeCastIntToUnsignedShort( 10 ));
        Assertions.assertEquals( -1, Numbers.safeCastIntToUnsignedShort( (Short.MAX_VALUE << 1) + 1 ) );
    }

    @Test
    void castIntToShort()
    {
        Assertions.assertEquals(1, Numbers.safeCastIntToShort( 1 ));
        Assertions.assertEquals(10, Numbers.safeCastIntToShort( 10 ));
        Assertions.assertEquals( Short.MAX_VALUE, Numbers.safeCastIntToShort( Short.MAX_VALUE ) );
        Assertions.assertEquals( Short.MIN_VALUE, Numbers.safeCastIntToShort( Short.MIN_VALUE ) );
    }

    @Test
    void castLongToByte()
    {
        Assertions.assertEquals(1, Numbers.safeCastLongToByte( 1L ));
        Assertions.assertEquals(10, Numbers.safeCastLongToByte( 10L ));
        Assertions.assertEquals(-1, Numbers.safeCastLongToByte( -1L ));
        Assertions.assertEquals(Byte.MAX_VALUE, Numbers.safeCastLongToByte( Byte.MAX_VALUE ));
        Assertions.assertEquals(Byte.MIN_VALUE, Numbers.safeCastLongToByte( Byte.MIN_VALUE ));
    }

    @Test
    void castUnsignedShortToInt()
    {
        Assertions.assertEquals( 1, Numbers.unsignedShortToInt( (short) 1 ) );
        Assertions.assertEquals( Short.MAX_VALUE, Numbers.unsignedShortToInt( Short.MAX_VALUE ) );
        Assertions.assertEquals( (Short.MAX_VALUE << 1) | 1, Numbers.unsignedShortToInt( (short) -1 ) );
    }

    @Test
    void checkLongCeilingPowerOfTwo()
    {
        Assertions.assertEquals( 1L, Numbers.ceilingPowerOfTwo( 1L ) );
        Assertions.assertEquals( 2L, Numbers.ceilingPowerOfTwo( 2L ) );
        Assertions.assertEquals( 8L, Numbers.ceilingPowerOfTwo( 5L ) );
        Assertions.assertEquals( 32L, Numbers.ceilingPowerOfTwo( 32L ) );
        Assertions.assertEquals( 1024L, Numbers.ceilingPowerOfTwo( 1023L ) );
        assertThrows( IllegalArgumentException.class, () -> Numbers.ceilingPowerOfTwo( Long.MAX_VALUE ) );
        assertThrows( IllegalArgumentException.class, () -> Numbers.ceilingPowerOfTwo( 0L ) );
        assertThrows( IllegalArgumentException.class, () -> Numbers.ceilingPowerOfTwo( -1L ) );
        assertThrows( IllegalArgumentException.class, () -> Numbers.ceilingPowerOfTwo( Long.MIN_VALUE ) );
    }

    @Test
    void checkIntCeilingPowerOfTwo()
    {
        Assertions.assertEquals( 1, Numbers.ceilingPowerOfTwo( 1 ) );
        Assertions.assertEquals( 2, Numbers.ceilingPowerOfTwo( 2 ) );
        Assertions.assertEquals( 8, Numbers.ceilingPowerOfTwo( 5 ) );
        Assertions.assertEquals( 32, Numbers.ceilingPowerOfTwo( 32 ) );
        Assertions.assertEquals( 1024, Numbers.ceilingPowerOfTwo( 1023 ) );
        assertThrows( IllegalArgumentException.class, () -> Numbers.ceilingPowerOfTwo( Integer.MAX_VALUE ) );
        assertThrows( IllegalArgumentException.class, () -> Numbers.ceilingPowerOfTwo( 0 ) );
        assertThrows( IllegalArgumentException.class, () -> Numbers.ceilingPowerOfTwo( -1 ) );
        assertThrows( IllegalArgumentException.class, () -> Numbers.ceilingPowerOfTwo( Integer.MIN_VALUE ) );
    }

    @Test
    void checkPowerOfTwo()
    {
        Assertions.assertTrue( Numbers.isPowerOfTwo( 1 ) );
        Assertions.assertFalse( Numbers.isPowerOfTwo( 5 ) );
        Assertions.assertTrue( Numbers.isPowerOfTwo( 8 ) );
        Assertions.assertTrue( Numbers.isPowerOfTwo( 1024 ) );
        Assertions.assertFalse( Numbers.isPowerOfTwo( Long.MAX_VALUE ) );
        assertThrows( IllegalArgumentException.class, () -> Numbers.isPowerOfTwo( 0 ) );
        assertThrows( IllegalArgumentException.class, () -> Numbers.isPowerOfTwo( -1 ) );
        assertThrows( IllegalArgumentException.class, () -> Numbers.isPowerOfTwo( Long.MIN_VALUE ) );
    }
}
