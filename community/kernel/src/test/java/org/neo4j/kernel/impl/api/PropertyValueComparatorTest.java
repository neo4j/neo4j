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

import org.junit.Test;

import static java.lang.String.format;
import static java.lang.System.out;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class PropertyValueComparatorTest
{
    @Test
    public void shouldDetectEmptyRanges()
    {
        // Set DEBUG=true below to see how this test works

        for ( boolean includeLower : BOOLEANS )
        {
            for ( boolean includeUpper : BOOLEANS )
            {
                for ( int lower : BOUND_VALUES )
                {
                    for ( int upper : BOUND_VALUES )
                    {
                        if ( COMPARATOR.isEmptyRange( lower, includeLower, upper, includeUpper ) )
                        {
                            assertRangeExcludesAllValues( includeLower, lower, includeUpper, upper );
                        }
                        else
                        {
                            assertRangeIncludesSomeValue( includeLower, lower, includeUpper, upper );
                        }
                        debug( "\n" );
                    }
                }
            }
        }
    }

    @Test
    public void shouldHandleNulls()
    {
        for ( boolean includeLower : BOOLEANS )
        {
            for ( boolean includeUpper : BOOLEANS )
            {
                assertFalse( COMPARATOR.isEmptyRange( null, includeLower, 10, includeUpper ) );
                assertFalse( COMPARATOR.isEmptyRange( 10, includeLower, null, includeUpper ) );
                assertFalse( COMPARATOR.isEmptyRange( null, includeLower, null, includeUpper ) );
            }
        }
    }

    private void assertRangeExcludesAllValues( boolean includeLower, int lower, boolean includeUpper, int upper )
    {
        debug( format( "Checking empty range %d %s x %s %d (cmp: %d)... Excludes... ",
                lower, includeLower ? ">=" : ">",
                includeUpper ? "<=" : "<", upper,
                Integer.compare( lower, upper )
        ) );

        for ( double value : TESTED_VALUES )
        {
            assertFalse( rangeIncludesGivenValue( includeLower, lower, includeUpper, upper, value) );
        }
    }

    private void assertRangeIncludesSomeValue( boolean includeLower, int lower, boolean includeUpper, int upper )
    {
        debug( format( "Checking non-empty range %d %s x %s %d (cmp: %d)... Includes... ",
                lower, includeLower ? ">=" : ">",
                includeUpper ? "<=" : "<", upper,
                Integer.compare( lower, upper )
        ) );


        boolean includesAny = false;
        for ( double value : TESTED_VALUES )
        {
            includesAny |= rangeIncludesGivenValue( includeLower, lower, includeUpper, upper, value );
        }
        assertTrue( includesAny );
    }

    private boolean rangeIncludesGivenValue( boolean includeLower, int lower, boolean includeUpper, int upper,
                                             double value )
    {
        boolean lowerInRange = includeLower ? (value >= lower) : (value > lower);
        boolean upperInRange = includeUpper ? (value <= upper) : (value < upper);
        if ( lowerInRange && upperInRange )
        {
            debug( format( "%s ", value ) );
            return true;
        }
        return false;
    }

    private static void debug( String text )
    {
        if ( DEBUG )
        {
            out.print( text );
        }
    }

    private final PropertyValueComparator<Integer> COMPARATOR = new PropertyValueComparator<Integer>()
    {
        @Override
        public int compare( Integer x, Integer y )
        {
            return Integer.compare( x, y );
        }
    };


    private final static boolean DEBUG = false;

    private final static boolean[] BOOLEANS = new boolean[]{true, false};
    private final static int LOWER = 10;
    private final static int UPPER = 20;

    private final static int[] BOUND_VALUES = new int[]{10, 11, 12, 13, 14};

    // This is needed to handle cases like 9 > x < 10, i.e. the bounds are very close and we need
    // to construct the in-between value by adding 0.5d
    private final static double[] TESTED_VALUES =
        new double[]{7.5, 8.5, 9, 9.5, 10, 10.5, 11, 11.5, 12, 12.5, 13, 13.5, 14, 14.5, 15, 15.5, 16, 16.5};
}

