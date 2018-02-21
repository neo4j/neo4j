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
package org.neo4j.values.utils;

import org.neo4j.values.storable.NumberValue;
import org.neo4j.values.storable.Values;

import static org.neo4j.values.storable.Values.doubleValue;
import static org.neo4j.values.storable.Values.longValue;

/**
 * Helper methods for doing math on Values
 */
public final class ValueMath
{
    private ValueMath()
    {
        throw new UnsupportedOperationException( "Do not instantiate" );
    }

    /**
     * Overflow safe addition of two longs
     * <p>
     * If the result doesn't fit in a long we widen type to use double instead.
     *
     * @param a left-hand operand
     * @param b right-hand operand
     * @return a + b
     */
    public static NumberValue add( long a, long b )
    {
        long r = a + b;
        //Check if result overflows
        if ( ((a ^ r) & (b ^ r)) < 0 )
        {
            return Values.doubleValue( (double) a + (double) b );
        }
        return longValue( r );
    }

    /**
     * Addition of two doubles
     *
     * @param a left-hand operand
     * @param b right-hand operand
     * @return a + b
     */
    public static NumberValue add( double a, double b )
    {
        return Values.doubleValue( a + b );
    }

    /**
     * Overflow safe subtraction of two longs
     * <p>
     * If the result doesn't fit in a long we widen type to use double instead.
     *
     * @param a left-hand operand
     * @param b right-hand operand
     * @return a + b
     */
    public static NumberValue subtract( long a, long b )
    {
        long r = a - b;
        //Check if result overflows
        if ( ((a ^ r) & (b ^ r)) < 0 )
        {
            return Values.doubleValue( (double) a - (double) b );
        }
        return longValue( r );
    }

    /**
     * Subtraction of two doubles
     *
     * @param a left-hand operand
     * @param b right-hand operand
     * @return a - b
     */
    public static NumberValue subtract( double a, double b )
    {
        return Values.doubleValue( a - b );
    }

    /**
     * Overflow safe multiplication of two longs
     * <p>
     * If the result doesn't fit in a long we widen type to use double instead.
     *
     * @param a left-hand operand
     * @param b right-hand operand
     * @return a * b
     */
    public static NumberValue multiply( long a, long b )
    {
        long r = a * b;
        //Check if result overflows
        long aa = Math.abs( a );
        long ab = Math.abs( b );
        if ( (aa | ab) >>> 31 != 0 )
        {
            if ( ((b != 0) && (r / b != a)) || (a == Long.MIN_VALUE && b == -1) )
            {
                return doubleValue( (double) a + (double) b );
            }
        }
        return longValue( r );
    }

    /**
     * Multiplication of two doubles
     *
     * @param a left-hand operand
     * @param b right-hand operand
     * @return a * b
     */
    public static NumberValue multiply( double a, double b )
    {
        return Values.doubleValue( a * b );
    }
}
