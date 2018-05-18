/*
 * Copyright (c) 2002-2018 "Neo4j,"
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
package org.neo4j.cypher.internal.runtime;

public class LongArrayHash
{

    public static final long NOT_IN_USE = -2;
    public static final int SLOT_EMPTY = 0;
    public static final int VALUE_FOUND = 1;
    public static final int CONTINUE_PROBING = -1;

    public static int hashCode( long[] arr, int from, int numberOfElements )
    {
        // This way of producing a hashcode for an array of longs is the
        // same used by java.util.Arrays.hashCode(long[])
        int h = 1;
        for ( int i = from; i < from + numberOfElements; i++ )
        {
            long element = arr[i];
            int elementHash = (int) (element ^ (element >>> 32));
            h = 31 * h + elementHash;
        }

        return h;
    }

    static boolean validValue( long[] arr, int width )
    {
        if ( arr.length != width )
        {
            throw new AssertionError( "all elements in the set must have the same size" );
        }
        for ( long l : arr )
        {
            if ( l == -1 || l == -2 )
            {
                throw new AssertionError( "magic values -1 and -2 not allowed in keys" );
            }
        }
        return true;
    }
}
