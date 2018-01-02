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
package org.neo4j.kernel.impl.api;

import java.util.Comparator;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import static java.lang.String.format;

import static org.neo4j.kernel.impl.api.PropertyValueComparison.COMPARE_NUMBERS;
import static org.neo4j.kernel.impl.api.PropertyValueComparison.COMPARE_STRINGS;
import static org.neo4j.kernel.impl.api.PropertyValueComparison.COMPARE_VALUES;
import static org.neo4j.kernel.impl.api.PropertyValueComparison.SuperType.*;

public class PropertyValueComparisonTest
{
    @Rule
    public ExpectedException thrown = ExpectedException.none();

    Object[] values = new Object[]{
            // OTHER
            PropertyValueComparison.LOWEST_OBJECT,
            new Object(),

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

    @Test
    public void valueComparatorShouldRejectLeftNullArgument()
    {
        thrown.expect( NullPointerException.class );
        PropertyValueComparison.COMPARE_VALUES.compare( null, 1 );
    }

    @Test
    public void valueComparatorShouldRejectRightNullArgument()
    {
        thrown.expect( NullPointerException.class );
        PropertyValueComparison.COMPARE_VALUES.compare( 1, null );
    }

    @Test
    public void numberComparatorShouldRejectLeftNullArgument()
    {
        thrown.expect( NullPointerException.class );
        PropertyValueComparison.COMPARE_NUMBERS.compare( null, 1 );
    }

    @Test
    public void numberComparatorShouldRejectRightNullArgument()
    {
        thrown.expect( NullPointerException.class );
        PropertyValueComparison.COMPARE_NUMBERS.compare( 1, null );
    }

    @Test
    public void stringComparatorShouldRejectLeftNullArgument()
    {
        thrown.expect( NullPointerException.class );
        PropertyValueComparison.COMPARE_STRINGS.compare( null, "foo" );
    }

    @Test
    public void stringComparatorShouldRejectRightNullArgument()
    {
        thrown.expect( NullPointerException.class );
        PropertyValueComparison.COMPARE_STRINGS.compare( "foo", null );
    }

    @Test
    public void shouldOrderValuesCorrectly()
    {
        for ( int i = 0; i < values.length; i++ )
        {
            for ( int j = 0; j < values.length; j++ )
            {
                Object left = values[i];
                Object right = values[j];

                int cmpPos = sign( i - j );
                int cmpVal = sign( compare( COMPARE_VALUES, left, right ) );

//                System.out.println( format( "%s (%d), %s (%d) => %d (%d)", left, i, right, j, cmpLeft, i - j ) );

                if ( cmpPos != cmpVal)
                {
                    throw new AssertionError( format(
                            "Comparing %s against %s does not agree with their positions in the sorted list (%d and " +
                                    "%d)",
                            left, right, i, j
                    ) );
                }

                if ( NUMBER.isSuperTypeOf( left ) && NUMBER.isSuperTypeOf( right ) )
                {
                    int cmpNum = sign( compare( COMPARE_NUMBERS, (Number) left, (Number) right ) );

                    if ( cmpPos != cmpNum )
                    {
                        throw new AssertionError( format(
                                "Comparing %s against %s numerically does not agree with their positions in the " +
                                        "sorted list (%d and %d)",
                                left, right, i, j
                        ) );
                    }
                }

                if ( STRING.isSuperTypeOf( left ) && STRING.isSuperTypeOf( right ) )
                {
                    int cmpNum = sign( compare( COMPARE_STRINGS, left, right ) );

                    if ( cmpPos != cmpNum )
                    {
                        throw new AssertionError( format(
                                "Comparing %s against %s textually does not agree with their positions in the " +
                                        "sorted list (%d and %d)",
                                left, right, i, j
                        ) );
                    }
                }
            }
        }
    }

    private <T> int compare( Comparator<T> comparator, T left, T right )
    {
        int cmp1 = comparator.compare( left, right );
        int cmp2 = comparator.compare( right, left );
        if ( sign( cmp1 ) != -sign( cmp2 ) )
        {
            throw new AssertionError( format( "%s is not symmetric on %s and %s", comparator, left, right ) );
        }
        return cmp1;
    }

    private int sign( int value )
    {
        return value == 0 ? 0 : (value < 0 ? -1 : +1);
    }
}
