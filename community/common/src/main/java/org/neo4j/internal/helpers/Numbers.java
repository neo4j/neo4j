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

import static org.neo4j.util.Preconditions.requirePositive;

public final class Numbers
{
    private static final long MAX_POWER_OF_TWO_LONG = 1L << (Long.SIZE - 2);
    private static final long MAX_POWER_OF_TWO_INTEGER = 1 << (Integer.SIZE - 2);

    private Numbers()
    {
    }

    /**
     * Checks if {@code value} is a power of 2.
     * @param value the value to check
     * @return {@code true} if {@code value} is a power of 2.
     */
    public static boolean isPowerOfTwo( long value )
    {
        requirePositive( value );
        return (value & (value - 1)) == 0;
    }

    /**
     * Calculate smallest power of two that is bigger or equal to provided value.
     * Provided value should be positive.
     * @param value user provided value
     * @return smallest power of two that is bigger or equal to provided value
     */
    public static long ceilingPowerOfTwo( long value )
    {
        requirePositive( value );
        if ( value > MAX_POWER_OF_TWO_LONG )
        {
            throw new IllegalArgumentException( "Provided value " + value + " is bigger than the biggest power of two long value." );
        }
        return 1L << -Long.numberOfLeadingZeros( value - 1 );
    }

    /**
     * Calculate smallest power of two that is bigger or equal to provided value.
     * Provided value should be positive.
     * @param value user provided value
     * @return smallest power of two that is bigger or equal to provided value
     */
    public static int ceilingPowerOfTwo( int value )
    {
        requirePositive( value );
        if ( value > MAX_POWER_OF_TWO_INTEGER )
        {
            throw new IllegalArgumentException( "Provided value " + value + " is bigger than the biggest power of two long value." );
        }
        return 1 << -Integer.numberOfLeadingZeros( value - 1 );
    }

    /**
     * Returns base 2 logarithm of the closest power of 2 that is less or equal to the {@code value}.
     *
     * @param value a positive long value
     */
    public static int log2floor( long value )
    {
        return (Long.SIZE - 1) - Long.numberOfLeadingZeros( requirePositive( value ) );
    }

    public static short safeCastIntToUnsignedShort( int value )
    {
        if ( (value & ~0xFFFF) != 0 )
        {
            throw new ArithmeticException( getOverflowMessage( value, "unsigned short" ) );
        }
        return (short) value;
    }

    public static byte safeCastIntToUnsignedByte( int value )
    {
        if ( (value & ~0xFF) != 0 )
        {
            throw new ArithmeticException( getOverflowMessage( value, "unsigned byte" ) );
        }
        return (byte) value;
    }

    public static int safeCastLongToInt( long value )
    {
        if ( (int) value != value )
        {
            throw new ArithmeticException( getOverflowMessage( value, Integer.TYPE ) );
        }
        return (int) value;
    }

    public static short safeCastLongToShort( long value )
    {
        if ( (short) value != value )
        {
            throw new ArithmeticException( getOverflowMessage( value, Short.TYPE ) );
        }
        return (short) value;
    }

    public static short safeCastIntToShort( int value )
    {
        if ( (short) value != value )
        {
            throw new ArithmeticException( getOverflowMessage( value, Short.TYPE ) );
        }
        return (short) value;
    }

    public static byte safeCastLongToByte( long value )
    {
        if ( (byte) value != value )
        {
            throw new ArithmeticException( getOverflowMessage( value, Byte.TYPE ) );
        }
        return (byte) value;
    }

    public static int unsignedShortToInt( short value )
    {
        return value & 0xFFFF;
    }

    public static int unsignedByteToInt( byte value )
    {
        return value & 0xFF;
    }

    private static String getOverflowMessage( long value, Class<?> clazz )
    {
        return getOverflowMessage( value, clazz.getName() );
    }

    private static String getOverflowMessage( long value, String numericType )
    {
        return "Value " + value + " is too big to be represented as " + numericType;
    }
}
