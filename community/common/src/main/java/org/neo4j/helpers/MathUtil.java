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
package org.neo4j.helpers;

import java.math.BigDecimal;
import java.util.Arrays;

public class MathUtil
{
    private static final long NON_DOUBLE_LONG = 0xFFE0_0000_0000_0000L; // doubles are exact integers up to 53 bits

    private MathUtil()
    {
        throw new AssertionError();
    }

    /**
     * Calculates the portion of the first value to all values passed
     * @param n The values in the set
     * @return the ratio of n[0] to the sum all n, 0 if result is {@link Double#NaN}
     */
    public static double portion( double... n )
    {
        assert n.length > 0;

        double first = n[0];
        if ( numbersEqual( first, 0 ) )
        {
            return 0d;
        }
        double total = Arrays.stream(n).sum();
        return first / total;
    }

    public static boolean numbersEqual( double fpn, long in )
    {
        if ( in < 0 )
        {
            if ( fpn < 0.0 )
            {
                if ( (NON_DOUBLE_LONG & in) == NON_DOUBLE_LONG ) // the high order bits are only sign bits
                { // no loss of precision if converting the long to a double, so it's safe to compare as double
                    return fpn == in;
                }
                else if ( fpn < Long.MIN_VALUE )
                { // the double is too big to fit in a long, they cannot be equal
                    return false;
                }
                else if ( (fpn == Math.floor( fpn )) && !Double.isInfinite( fpn ) ) // no decimals
                { // safe to compare as long
                    return in == (long) fpn;
                }
            }
        }
        else
        {
            if ( !(fpn < 0.0) )
            {
                if ( (NON_DOUBLE_LONG & in) == 0 ) // the high order bits are only sign bits
                { // no loss of precision if converting the long to a double, so it's safe to compare as double
                    return fpn == in;
                }
                else if ( fpn > Long.MAX_VALUE )
                { // the double is too big to fit in a long, they cannot be equal
                    return false;
                }
                else if ( (fpn == Math.floor( fpn )) && !Double.isInfinite( fpn ) )  // no decimals
                { // safe to compare as long
                    return in == (long) fpn;
                }
            }
        }
        return false;
    }

    // Tested by PropertyValueComparisonTest
    public static int compareDoubleAgainstLong( double lhs, long rhs )
    {
        if  ( (NON_DOUBLE_LONG & rhs ) != NON_DOUBLE_LONG )
        {
            if ( Double.isNaN( lhs ) )
            {
                return +1;
            }
            if ( Double.isInfinite( lhs ) )
            {
                return lhs < 0 ? -1 : +1;
            }
            return BigDecimal.valueOf( lhs ).compareTo( BigDecimal.valueOf( rhs ) );
        }
        return Double.compare( lhs, rhs );
    }

    // Tested by PropertyValueComparisonTest
    public static int compareLongAgainstDouble( long lhs, double rhs )
    {
        return - compareDoubleAgainstLong( rhs, lhs );
    }

    /**
     * Return an integer one less than the given integer, or throw {@link ArithmeticException} if the given integer is
     * zero.
     *
     * @param value integer to decrement
     * @return the provided integer minus one
     * @throws ArithmeticException if the resulting integer would be less than zero
     */
    public static int decrementExactNotPastZero( int value )
    {
        if ( value == 0 )
        {
            throw new ArithmeticException( "integer underflow past zero" );
        }
        return value - 1;
    }
}

