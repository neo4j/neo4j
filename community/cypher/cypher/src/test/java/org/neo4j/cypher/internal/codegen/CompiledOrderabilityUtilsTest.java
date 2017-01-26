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
package org.neo4j.cypher.internal.codegen;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.Arrays;

import org.neo4j.cypher.internal.frontend.v3_2.IncomparableValuesException;
import org.neo4j.kernel.impl.api.PropertyValueComparison;

import static java.lang.String.format;

/**
 * Inspired by {@link org.neo4j.kernel.impl.api.PropertyValueComparisonTest}
 */
public class CompiledOrderabilityUtilsTest
{
    @Rule
    public ExpectedException thrown = ExpectedException.none();

    // TODO add acceptance tests too

    public static Object[] values = new Object[]{
            // OTHER
            PropertyValueComparison.LOWEST_OBJECT,
            new Object(),
            new Object[]{1, "foo"},
            new Object[]{1, "foo", 3},
            new Object[]{1, 2, "bar"},
            new Object[]{1, 2, "car"},
            // TODO fails when this is uncommented
//            new int[]{1, 2, 3},
            // STRING
            "",
            Character.MIN_VALUE,
            " ",
            "20",
            "x",
            "y",
            Character.MIN_HIGH_SURROGATE,
            Character.MAX_HIGH_SURROGATE,
            Character.MIN_LOW_SURROGATE,
            Character.MAX_LOW_SURROGATE,
            Character.MAX_VALUE,

            // BOOLEAN
            false,
            true,

            // NUMBER
            Double.NEGATIVE_INFINITY,
            -Double.MAX_VALUE,
            Long.MIN_VALUE,
            Long.MIN_VALUE + 1,
            Integer.MIN_VALUE,
            Short.MIN_VALUE,
            Byte.MIN_VALUE,
            0,
            Double.MIN_VALUE,
            Double.MIN_NORMAL,
            Float.MIN_VALUE,
            Float.MIN_NORMAL,
            1L,
            1.1d,
            1.2f,
            Math.E,
            Math.PI,
            (byte) 10,
            (short) 20,
            Byte.MAX_VALUE,
            Short.MAX_VALUE,
            Integer.MAX_VALUE,
            9007199254740992D,
            9007199254740993L,
            Long.MAX_VALUE,
            Float.MAX_VALUE,
            Double.MAX_VALUE,
            Double.POSITIVE_INFINITY,
            Double.NaN
    };

//    // TODO investigate: fails
//    public static Object[] values = new Object[]{
//            // OTHER
//            new Object[]{1, "foo"},
//            new Object[]{1, "foo", 3},
//            new Object[]{1, 2, "bar"},
//            new int[]{1, 2, 3}
//    };

//    // TODO investigate: passes
//    public static Object[] values = new Object[]{
//            // OTHER
//            new Object[]{1, "foo"},
//            new Object[]{1, "foo", 3},
//            new Object[]{1, 2, "bar"},
//            new Object[]{1, 2, 3}
//    };

    @Test
    public void shouldOrderValuesCorrectly()
    {
        for ( int i = 2; i < values.length; i++ )
        {
            for ( int j = 2; j < values.length; j++ )
            {
                Object left = values[i];
                Object right = values[j];

                int cmpPos = sign( i - j );
                int cmpVal = sign( compare( left, right ) );

                if ( cmpPos != cmpVal )
                {
                    throw new AssertionError( format(
                            "Comparing %s against %s does not agree with their positions in the sorted list (%d and " +
                            "%d)",
                            toString( left ), toString( right ), i, j
                    ) );
                }
            }
        }
    }

    private String toString( Object o )
    {
        Class clazz = o.getClass();
        if ( clazz.equals( Object[].class ) )
        {
            return Arrays.toString( (Object[]) o );
        }
        else if ( clazz.equals( int[].class ) )
        {
            return Arrays.toString( (int[]) o );
        }
        else if ( clazz.equals( Integer[].class ) )
        {
            return Arrays.toString( (Integer[]) o );
        }
        else if ( clazz.equals( long[].class ) )
        {
            return Arrays.toString( (long[]) o );
        }
        else if ( clazz.equals( Long[].class ) )
        {
            return Arrays.toString( (Long[]) o );
        }
        else if ( clazz.equals( String[].class ) )
        {
            return Arrays.toString( (String[]) o );
        }
        else if ( clazz.equals( boolean[].class ) )
        {
            return Arrays.toString( (boolean[]) o );
        }
        else if ( clazz.equals( Boolean[].class ) )
        {
            return Arrays.toString( (Boolean[]) o );
        }
        else
        {
            return o.toString();
        }
    }

    private <T> int compare( T left, T right )
    {
        try
        {
            int cmp1 = CompiledOrderabilityUtils.compare( left, right );
            int cmp2 = CompiledOrderabilityUtils.compare( right, left );
            if ( sign( cmp1 ) != -sign( cmp2 ) )
            {
                throw new AssertionError( format( "Comparator is not symmetric on %s and %s", left, right ) );
            }
            return cmp1;
        }
        catch ( IncomparableValuesException e )
        {
            throw new AssertionError(
                    format( "Failed to compare %s:%s and %s:%s", left, left.getClass().getName(), right,
                            right.getClass().getName() ), e );
        }
    }

    private int sign( int value )
    {
        return value == 0 ? 0 : (value < 0 ? -1 : +1);
    }
}
