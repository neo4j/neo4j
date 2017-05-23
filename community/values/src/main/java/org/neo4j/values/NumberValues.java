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
package org.neo4j.values;

import java.math.BigDecimal;

@SuppressWarnings( "WeakerAccess" )
public class NumberValues
{
    private NumberValues()
    {
    }

    private static final long NON_DOUBLE_LONG = 0xFFE0_0000_0000_0000L; // doubles are exact integers up to 53 bits

    public static int hash( long number )
    {
        return (int) (number ^ (number >>> 32));
    }

    public static int hash( double number )
    {
        long asLong = (long) number;
        if ( asLong == number )
        {
            return hash( asLong );
        }
        long bits = Double.doubleToLongBits( number );
        return (int)(bits ^ (bits >>> 32));
    }

    public static int hash( byte[] values )
    {
        int result = 1;
        for ( byte value : values )
        {
            int elementHash = NumberValues.hash( value );
            result = 31 * result + elementHash;
        }
        return result;
    }

    public static int hash( short[] values )
    {
        int result = 1;
        for ( short value : values )
        {
            int elementHash = NumberValues.hash( value );
            result = 31 * result + elementHash;
        }
        return result;
    }

    public static int hash( int[] values )
    {
        int result = 1;
        for ( int value : values )
        {
            int elementHash = NumberValues.hash( value );
            result = 31 * result + elementHash;
        }
        return result;
    }

    public static int hash( long[] values )
    {
        int result = 1;
        for ( long value : values )
        {
            int elementHash = NumberValues.hash( value );
            result = 31 * result + elementHash;
        }
        return result;
    }

    public static int hash( float[] values )
    {
        int result = 1;
        for ( float value : values )
        {
            int elementHash = NumberValues.hash( value );
            result = 31 * result + elementHash;
        }
        return result;
    }

    public static int hash( double[] values )
    {
        int result = 1;
        for ( double value : values )
        {
            int elementHash = NumberValues.hash( value );
            result = 31 * result + elementHash;
        }
        return result;
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
     */
    public static int decrementExactNotPastZero( int value )
    {
        if ( value == 0 )
        {
            throw new ArithmeticException( "integer underflow past zero" );
        }
        return value - 1;
    }

    public static boolean numbersEqual( ValueGroup.VIntegerArray lhs, ValueGroup.VIntegerArray rhs )
    {
        int length = lhs.length();
        if ( length != rhs.length() )
        {
            return false;
        }
        for ( int i = 0; i < length; i++ )
        {
            if ( lhs.longValue( i ) != rhs.longValue( i ) )
            {
                return false;
            }
        }
        return true;
    }

    public static boolean numbersEqual( ValueGroup.VFloatingPointArray lhs, ValueGroup.VFloatingPointArray rhs )
    {
        int length = lhs.length();
        if ( length != rhs.length() )
        {
            return false;
        }
        for ( int i = 0; i < length; i++ )
        {
            if ( lhs.doubleValue( i ) != rhs.doubleValue( i ) )
            {
                return false;
            }
        }
        return true;
    }

    public static boolean numbersEqual( ValueGroup.VFloatingPointArray fps, ValueGroup.VIntegerArray ins )
    {
        int length = ins.length();
        if ( length != fps.length() )
        {
            return false;
        }
        for ( int i = 0; i < length; i++ )
        {
            if ( !numbersEqual( fps.doubleValue( i ), ins.longValue( i ) ) )
            {
                return false;
            }
        }
        return true;
    }

    public static int compareIntegerArrays( ValueGroup.VIntegerArray a, ValueGroup.VIntegerArray b )
    {
        int i = 0;
        int length = a.length();
        int x = length - b.length();

        while ( x == 0 && i < length )
        {
            x = Long.compare( a.longValue( i ), b.longValue( i ) );
            i++;
        }
        return x;
    }

    public static int compareIntegerVsFloatArrays( ValueGroup.VIntegerArray a, ValueGroup.VFloatingPointArray b )
    {
        int i = 0;
        int length = a.length();
        int x = length - b.length();

        while ( x == 0 && i < length )
        {
            x = compareLongAgainstDouble( a.longValue( i ), b.doubleValue( i ) );
            i++;
        }
        return x;
    }

    public static int compareFloatArrays( ValueGroup.VFloatingPointArray a, ValueGroup.VFloatingPointArray b )
    {
        int i = 0;
        int length = a.length();
        int x = length - b.length();

        while ( x == 0 && i < length )
        {
            x = Double.compare( a.doubleValue( i ), b.doubleValue( i ) );
            i++;
        }
        return x;
    }
}
