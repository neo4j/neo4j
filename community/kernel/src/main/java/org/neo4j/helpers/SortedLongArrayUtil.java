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

import static java.util.Arrays.copyOf;

/**
 * Specialized methods for operations of sorted long arrays.
 */
public class SortedLongArrayUtil
{
    /**
     * Compute union of two sorted long arrays.
     * @param left one sorted array
     * @param right the other sorted array
     * @return the union, which is NOT sorted
     */
    public static long[] union( long[] left, long[] right )
    {
        if ( left == null || right == null )
        {
            return left == null ? right : left;
        }

        int missing = missing( left, right );
        if ( missing == 0 )
        {
            return left;
        }

        // An attempt to add the labels as efficiently as possible
        long[] union = copyOf( left, left.length + missing );

        int i = 0;
        int cursor = left.length;
        for ( long candidate : right )
        {
            while ( i < left.length && left[i] < candidate )
            {
                i++;
            }
            if ( i == left.length || candidate != left[i] )
            {
                union[cursor++] = candidate;
                missing--;
            }
        }
        assert missing == 0;
        return union;
    }

    /**
     * Compute how many values in maybeMissing that are not in reference.
     * @param reference sorted set
     * @param maybeMissing sorted set
     * @return number of missing values
     */
    public static int missing( long[] reference, long[] maybeMissing )
    {
        int i = 0;
        int j = 0;
        int count = 0;
        while ( i < reference.length && j < maybeMissing.length )
        {
            if ( reference[i] == maybeMissing[j] )
            {
                i++;
                j++;
            }
            else if ( reference[i] < maybeMissing[j] )
            {
                i++;
            }
            else
            {
                count++;
                j++;
            }
        }
        count += maybeMissing.length - j;
        return count;
    }

    private SortedLongArrayUtil()
    {   // No instances allowed
    }
}
