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
package org.neo4j.util;

import static java.lang.String.format;
import static org.neo4j.helpers.Numbers.isPowerOfTwo;

/**
 * A set of static convenience methods for checking ctor/method parameters or state.
 */
public final class Preconditions
{
    private Preconditions()
    {
        throw new AssertionError( "no instances" );
    }

    /**
     * Ensures that {@code value} is greater than or equal to {@code 1} or throws {@link IllegalArgumentException} otherwise.
     *
     * @param value a value for check
     * @return {@code value} if it's greater than or equal to {@code 1}
     * @throws IllegalArgumentException if {@code value} is less than 1
     */
    public static long requirePositive( long value )
    {
        if ( value < 1 )
        {
            throw new IllegalArgumentException( "Expected positive long value, got " + value );
        }
        return value;
    }

    /**
     * Ensures that {@code value} is a power of 2 or throws {@link IllegalArgumentException} otherwise.
     *
     * @param value a value for check
     * @return {@code value} if it's a power of 2
     * @throws IllegalArgumentException if {@code value} is not power of 2
     */
    public static long requirePowerOfTwo( long value )
    {
        if ( !isPowerOfTwo( value ) )
        {
            throw new IllegalArgumentException( "Expected long value to be a non zero power of 2, got " + value );
        }
        return value;
    }

    /**
     * Ensures that {@code value} is greater than or equal to {@code 1} or throws {@link IllegalArgumentException} otherwise.
     *
     * @param value a value for check
     * @return {@code value} if it's greater than or equal to {@code 1}
     * @throws IllegalArgumentException if {@code value} is less than 1
     */
    public static int requirePositive( int value )
    {
        if ( value < 1 )
        {
            throw new IllegalArgumentException( "Expected positive int value, got " + value );
        }
        return value;
    }

    /**
     * Ensures that {@code value} is greater than or equal to {@code 0} or throws {@link IllegalArgumentException} otherwise.
     *
     * @param value a value for check
     * @return {@code value} if it's greater than or equal to {@code 0}
     * @throws IllegalArgumentException if {@code value} is less than 0
     */
    public static long requireNonNegative( long value )
    {
        if ( value < 0 )
        {
            throw new IllegalArgumentException( "Expected non-negative long value, got " + value );
        }
        return value;
    }

    /**
     * Ensures that {@code value} is greater than or equal to {@code 0} or throws {@link IllegalArgumentException} otherwise.
     *
     * @param value a value for check
     * @return {@code value} if it's greater than or equal to {@code 0}
     * @throws IllegalArgumentException if {@code value} is less than 0
     */
    public static int requireNonNegative( int value )
    {
        if ( value < 0 )
        {
            throw new IllegalArgumentException( "Expected non-negative int value, got " + value );
        }
        return value;
    }

    /**
     * Ensures that {@code value} is exactly zero.
     *
     * @param value a value for check
     * @return {@code value} if it's equal to {@code 0}.
     * @throws IllegalArgumentException if {@code value} is not 0
     */
    public static int requireExactlyZero( int value )
    {
        if ( value != 0 )
        {
            throw new IllegalArgumentException( "Expected int value equal to 0, got " + value );
        }
        return value;
    }

    /**
     * Ensures that {@code expression} is {@code true} or throws {@link IllegalStateException} otherwise.
     *
     * @param expression an expression for check
     * @param message error message for the exception
     * @throws IllegalStateException if {@code expression} is {@code false}
     */
    public static void checkState( boolean expression, String message )
    {
        if ( !expression )
        {
            throw new IllegalStateException( message );
        }
    }

    /**
     * Ensures that {@code expression} is {@code true} or throws {@link IllegalArgumentException} otherwise.
     *
     * @param expression an expression for check
     * @param message error message for the exception
     * @throws IllegalArgumentException if {@code expression} is {@code false}
     */
    public static void checkArgument( boolean expression, String message, Object... args )
    {
        if ( !expression )
        {
            throw new IllegalArgumentException( args.length > 0 ? format( message, args ) : message );
        }
    }

    public static void requireBetween( int value, int lowInclusive, int highExclusive )
    {
        if ( value < lowInclusive || value >= highExclusive )
        {
            throw new IllegalArgumentException( String.format( "Expected int value between %d (inclusive) and %d (exclusive), got %d.",
                    lowInclusive, highExclusive, value ) );
        }
    }
}
