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
package org.neo4j.helpers;

public class Numbers
{
    public static short safeCastIntToUnsignedShort( int value )
    {
        if ( (value & ~0xFFFF) != 0 )
        {
            throw new ArithmeticException( getOverflowMessage( value, "unsigned short" ) );
        }
        return (short) value;
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

    private static String getOverflowMessage( long value, Class clazz )
    {
        return getOverflowMessage( value, clazz.getName() );
    }

    private static String getOverflowMessage( long value, String numericType )
    {
        return "Value " + value + " is too big to be represented as " + numericType;
    }
}
