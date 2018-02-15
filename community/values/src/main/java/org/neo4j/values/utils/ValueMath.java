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
     * If the result doesn't fit in a long we widen type to use double instead. This implementation
     * is fast for the happy path since it is using the intrinsic method addExact but will be slow when
     * actually overflowing. We bet on overflows being rare and optimize for the happy path.
     *
     * @param a left-hand operand
     * @param b right-hand operand
     * @return a + b
     */
    public static NumberValue add( long a, long b )
    {
        try
        {
            return Values.longValue( Math.addExact( a, b ) );
        }
        catch ( ArithmeticException e )
        {
            return Values.doubleValue( (double) a + (double) b );
        }
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
     * If the result doesn't fit in a long we widen type to use double instead. This implementation
     * is fast for the happy path since it is using the intrinsic method addExact but will be slow when
     * actually overflowing. We bet on overflows being rare and optimize for the happy path.
     *
     * @param a left-hand operand
     * @param b right-hand operand
     * @return a + b
     */
    public static NumberValue subtract( long a, long b )
    {
        try
        {
            return Values.longValue( Math.subtractExact( a, b ) );
        }
        catch ( ArithmeticException e )
        {
            return Values.doubleValue( (double) a - (double) b );
        }
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
}
