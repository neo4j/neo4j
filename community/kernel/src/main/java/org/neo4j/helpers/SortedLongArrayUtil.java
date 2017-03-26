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

/**
 * Specialized methods for operations of sorted long arrays.
 */
public class SortedLongArrayUtil
{
    private static final long[] EMPTY = new long[]{};

    /**
     * Compute union of two sorted long arrays.
     * @param left a sorted array set
     * @param right another sorted array set
     * @return the union, which is NOT sorted
     */
    public static long[] union( long[] left, long[] right )
    {
        if ( left == null || right == null )
        {
            return left == null ? right : left;
        }

        long uniqueCounts = countUnique( left, right );
        if ( uniqueCounts == 0 || right( uniqueCounts ) == 0 )
        {
            return left;
        }
        if ( left( uniqueCounts ) == 0 )
        {
            return right;
        }

        long[] union = new long[left.length + right( uniqueCounts )];

        int cursor = 0;
        int l = 0;
        int r = 0;
        while ( l < left.length && r < right.length )
        {
            if ( left[l] == right[r] )
            {
                union[cursor++] = left[l];
                l++;
                r++;
            }
            else if ( left[l] < right[r] )
            {
                union[cursor++] = left[l];
                l++;
            }
            else
            {
                union[cursor++] = right[r];
                r++;
            }
        }
        while ( l < left.length )
        {
            union[cursor++] = left[l];
            l++;
        }
        while ( r < right.length )
        {
            union[cursor++] = right[r];
            r++;
        }

        assert cursor == union.length;
        return union;
    }

    /**
     * Compute intersect of two sorted long array sets.
     * @param left a sorted array set
     * @param right another sorted array set
     * @return the union, which is NOT sorted
     */
    public static long[] intersect( long[] left, long[] right )
    {
        if ( left == null || right == null )
        {
            return EMPTY;
        }

        long uniqueCounts = countUnique( left, right );
        if ( uniqueCounts == 0 ) // complete intersection
        {
            return right;
        }
        if ( right( uniqueCounts ) == right.length || left( uniqueCounts ) == left.length ) // non-intersecting
        {
            return EMPTY;
        }

        long[] intersect = new long[left.length - left( uniqueCounts )];

        int cursor = 0;
        int l = 0;
        int r = 0;
        while ( l < left.length && r < right.length )
        {
            if ( left[l] == right[r] )
            {
                intersect[cursor++] = left[l];
                l++;
                r++;
            }
            else if ( left[l] < right[r] )
            {
                l++;
            }
            else
            {
                r++;
            }
        }

        assert cursor == intersect.length;
        return intersect;
    }

    /**
     * Compute the symmetric difference (set XOR basically) of two sorted long array sets.
     * @param left a sorted array set
     * @param right another sorted array set
     * @return the union, which is NOT sorted
     */
    public static long[] symmetricDifference( long[] left, long[] right )
    {
        if ( left == null || right == null )
        {
            return left == null ? right : left;
        }

        long uniqueCounts = countUnique( left, right );
        if ( uniqueCounts == 0 ) // complete intersection
        {
            return EMPTY;
        }

        long[] difference = new long[left( uniqueCounts ) + right( uniqueCounts )];

        int cursor = 0;
        int l = 0;
        int r = 0;
        while ( l < left.length && r < right.length )
        {
            if ( left[l] == right[r] )
            {
                l++;
                r++;
            }
            else if ( left[l] < right[r] )
            {
                difference[cursor++] = left[l];
                l++;
            }
            else
            {
                difference[cursor++] = right[r];
                r++;
            }
        }
        while ( l < left.length )
        {
            difference[cursor++] = left[l];
            l++;
        }
        while ( r < right.length )
        {
            difference[cursor++] = right[r];
            r++;
        }

        assert cursor == difference.length;
        return difference;
    }

    /**
     * Compute the number of unique values in two sorted long array sets
     * @param left a sorted array set
     * @param right another sorted array set
     * @return int pair packed into long
     */
    public static long countUnique( long[] left, long[] right )
    {
        int l = 0;
        int r = 0;
        int uniqueInLeft = 0;
        int uniqueInRight = 0;
        while ( l < left.length && r < right.length )
        {
            if ( left[l] == right[r] )
            {
                l++;
                r++;
            }
            else if ( left[l] < right[r] )
            {
                uniqueInLeft++;
                l++;
            }
            else
            {
                uniqueInRight++;
                r++;
            }
        }
        uniqueInLeft += left.length - l;
        uniqueInRight += right.length - r;
        return intPair( uniqueInLeft, uniqueInRight );
    }

    public static long intPair( int left, int right )
    {
        return (long)left << (Integer.BYTES * 8) | right;
    }

    public static int left( long pair )
    {
        return (int)(pair >> (Integer.BYTES * 8));
    }

    public static int right( long pair )
    {
        return (int)pair;
    }

    private SortedLongArrayUtil()
    {   // No instances allowed
    }
}
