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

public class PrimitiveSortedArraySet
{
    /**
     * Merges two sets of integers represented as sorted arrays.
     *
     * @param lhs
     *         a set of integers, represented as a sorted array.
     * @param rhs
     *         a set of integers, represented as a sorted array.
     * @return a set of integers, represented as a sorted array.
     */
    public static int[] mergeSortedSet( int[] lhs, int[] rhs )
    {
        if ( lhs.length < rhs.length )
        {
            return mergeSortedSet( rhs, lhs );
        }
        int[] merged = null;
        int m = 0;
        for ( int l = 0, r = 0; l < lhs.length && r < rhs.length; )
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
        if ( merged.length < m )
        {
            merged = Arrays.copyOf( merged, m );
        }
        return merged;
    }
}
