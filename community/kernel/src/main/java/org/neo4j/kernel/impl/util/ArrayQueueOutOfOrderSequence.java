/**
 * Copyright (c) 2002-2014 "Neo Technology,"
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
package org.neo4j.kernel.impl.util;

import static java.lang.Math.max;
import static java.lang.String.format;

/**
 * A crude, synchronized implementation of OutOfOrderSequence. Please implement a faster one if need be.
 */
public class ArrayQueueOutOfOrderSequence implements OutOfOrderSequence
{
    private volatile long highestGapFreeNumber;
    private final SortedArray outOfOrderQueue;

    public ArrayQueueOutOfOrderSequence( long startingNumber, int initialArraySize )
    {
        this.highestGapFreeNumber = startingNumber;
        this.outOfOrderQueue = new SortedArray( initialArraySize );
    }

    @Override
    public synchronized void offer( long number )
    {
        if ( highestGapFreeNumber + 1 == number )
        {
            highestGapFreeNumber = outOfOrderQueue.pollHighestGapFree( number );
        }
        else
        {
            outOfOrderQueue.offer( highestGapFreeNumber, number );
        }
    }

    @Override
    public long get()
    {
        return highestGapFreeNumber;
    }

    @Override
    public synchronized void set( long number )
    {
        highestGapFreeNumber = number;
        outOfOrderQueue.clear();
    }

    @Override
    public synchronized String toString()
    {
        return format( "out-of-order-sequence:%d [%s]", highestGapFreeNumber, outOfOrderQueue );
    }

    private static class SortedArray
    {
        private static final long UNSET = -1L;
        // This is the backing store, treated as a ring courtesy of cursor
        private long[] array;
        private int cursor;
        private int length;

        public SortedArray( int initialArraySize )
        {
            this.array = new long[initialArraySize];
        }

        public void clear()
        {
            cursor = 0;
            length = 0;
        }

        void offer( long baseNumber, long number )
        {
            int diff = (int) (number-baseNumber);
            ensureArrayCapacity( diff );
            int index = cursor+diff-1;
            for ( int i = cursor+length; i < index; i++ )
            {
                array[i%array.length] = UNSET;
            }
            array[index%array.length] = number;
            length = max( length, diff );
        }

        long pollHighestGapFree( long given )
        {
            // assume that "given" would be placed at cursor
            long number = given;
            int length = this.length-1;
            for ( int i = 0; i < length; i++ )
            {
                int index = advanceCursor();
                if ( array[index] == UNSET )
                {
                    break;
                }

                number++;
                assert array[index] == number : "Expected index " + index + " to be " + number +
                        ", but was " + array[index] + ". This is for i=" + i;
            }
            return number;
        }

        private int advanceCursor()
        {
            cursor = (cursor+1)%array.length;
            length--;
            assert length >= 0;
            return cursor;
        }

        private void ensureArrayCapacity( int capacity )
        {
            while ( capacity > array.length )
            {
                long[] newArray = new long[array.length*2];
                // Copy contents to new array, newArray starting at 0
                for ( int i = 0; i < length; i++ )
                {
                    newArray[i] = array[(cursor+i)%array.length];
                }
                array = newArray;
                cursor = 0;
            }
        }

        @Override
        public String toString()
        {
            StringBuilder builder = new StringBuilder();
            for ( int i = 0; i < length; i++ )
            {
                long value = array[(cursor+i)%array.length];
                if ( value != UNSET )
                {
                    builder.append( builder.length() > 0 ? "," : "" ).append( value );
                }
            }
            return builder.toString();
        }
    }
}
