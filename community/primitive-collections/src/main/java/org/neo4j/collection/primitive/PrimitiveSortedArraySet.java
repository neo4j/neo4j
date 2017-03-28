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
package org.neo4j.collection.primitive;

import java.util.Arrays;

/**
 * Specialized methods for operations on sets represented as sorted primitive arrays.
 *
 * This class does not contain a complete set of operations for all primitives, but only
 * the ones that were needed. Feel free to add specializations on demand, but remember to test.
 */
public class PrimitiveSortedArraySet
{
    private static final long[] NO_LONGS = new long[]{};
    private static final int INT_BITS = Integer.BYTES * 8;

    /**
     * Compute union of two sets of integers represented as sorted arrays.
     *
     * @param lhs
     *         a set of integers, represented as a sorted array.
     * @param rhs
     *         a set of integers, represented as a sorted array.
     * @return a set of integers, represented as a sorted array.
     */
    // NOTE: this implementation was measured to be faster than an implementation
    // with countUnique for arrays on size 100+.
    public static int[] union( int[] lhs, int[] rhs )
    {
        if ( lhs == null || rhs == null )
        {
            return lhs == null ? rhs : lhs;
        }

        if ( lhs.length < rhs.length )
        {
            return union( rhs, lhs );
        }
        int[] merged = null;
        int m = 0;
        int l = 0;
        for ( int r = 0; l < lhs.length && r < rhs.length; )
        {
            while ( l < lhs.length && lhs[l] < rhs[r] )
            {
                if ( merged != null )
                {
                    merged[m++] = lhs[l];
                }
                l++;
            }
            if ( l == lhs.length )
            {
                if ( merged == null )
                {
                    merged = Arrays.copyOf( lhs, lhs.length + rhs.length - r );
                    m = l;
                }
                System.arraycopy( rhs, r, merged, m, rhs.length - r );
                m += rhs.length - r;
                break;
            }
            if ( lhs[l] > rhs[r] )
            {
                if ( merged == null )
                {
                    merged = Arrays.copyOf( lhs, lhs.length + rhs.length - r );
                    m = l;
                }
                merged[m++] = rhs[r++];
            }
            else // i.e. ( lhs[l] == rhs[r] )
            {
                if ( merged != null )
                {
                    merged[m++] = lhs[l];
                }
                l++;
                r++;
            }
        }
        if ( merged == null )
        {
            return lhs;
        }
        if ( l < lhs.length ) // get tail of lhs
        {
            System.arraycopy( lhs, l, merged, m, lhs.length - l );
            m += lhs.length - l;
        }
        if ( m < merged.length ) // truncate extra elements
        {
            merged = Arrays.copyOf( merged, m );
        }
        return merged;
    }

    /**
     * Compute the intersection of two sorted long array sets.
     * @param left a sorted array set
     * @param right another sorted array set
     * @return the intersection, represented as a sorted long array
     */
    public static long[] intersect( long[] left, long[] right )
    {
        if ( left == null || right == null )
        {
            return NO_LONGS;
        }

        long uniqueCounts = countUnique( left, right );
        if ( uniqueCounts == 0 ) // complete intersection
        {
            return right;
        }
        if ( right( uniqueCounts ) == right.length || left( uniqueCounts ) == left.length ) // non-intersecting
        {
            return NO_LONGS;
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
     * @return the symmetric difference, represented as a sorted long array
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
            return NO_LONGS;
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
    static long countUnique( long[] left, long[] right )
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

    private static long intPair( int left, int right )
    {
        return ( ((long)left) << INT_BITS ) | right;
    }

    private static int left( long pair )
    {
        return (int)(pair >> INT_BITS);
    }

    private static int right( long pair )
    {
        return (int)(pair & 0xFFFF_FFFFL);
    }

    private PrimitiveSortedArraySet()
    {   // No instances allowed
    }
}
