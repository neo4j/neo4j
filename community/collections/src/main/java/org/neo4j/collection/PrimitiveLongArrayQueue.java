/*
 * Copyright (c) 2002-2020 "Neo4j,"
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
package org.neo4j.collection;

import org.eclipse.collections.api.iterator.LongIterator;

import java.util.NoSuchElementException;

/**
 * Simple array based FIFO queue for primitive longs.
 * Newly enqueued element is added into the end of the queue, and dequeue will return
 * element from the head of the queue. (See CLRS 10.1 for more detailed description)
 *
 * Queue capacity should always be power of two to be able to use
 * '&' mask operation with {@link #values} length.
 */
public class PrimitiveLongArrayQueue
{
    private static final int DEFAULT_CAPACITY = 16;
    private long[] values;
    private int head;
    private int tail;

    public PrimitiveLongArrayQueue()
    {
        this( DEFAULT_CAPACITY );
    }

    PrimitiveLongArrayQueue( int capacity )
    {
        if ( capacity == 0 || (capacity & (capacity - 1)) != 0 )
        {
            throw new IllegalArgumentException( "Capacity should be power of 2. Requested capacity: " + capacity );
        }
        initValues( capacity );
    }

    public boolean isEmpty()
    {
        return head == tail;
    }

    public void clear()
    {
        initValues( DEFAULT_CAPACITY );
    }

    public int size()
    {
        return (tail - head) & (values.length - 1);
    }

    public LongIterator longIterator()
    {
        return new PrimitiveLongArrayQueueIterator();
    }

    public long dequeue()
    {
        if ( isEmpty() )
        {
            throw new IllegalStateException( "Fail to poll first element. Queue is empty." );
        }
        long value = values[head];
        head = (head + 1) & (values.length - 1);
        return value;
    }

    public void enqueue( long value )
    {
        values[tail] = value;
        tail = (tail + 1) & (values.length - 1);
        if ( tail == head )
        {
            ensureCapacity();
        }
    }

    public void addAll( PrimitiveLongArrayQueue otherQueue )
    {
        while ( !otherQueue.isEmpty() )
        {
            enqueue( otherQueue.dequeue() );
        }
    }

    private void initValues( int capacity )
    {
        values = new long[capacity];
        head = 0;
        tail = 0;
    }

    private void ensureCapacity()
    {
        int newCapacity = values.length << 1;
        if ( newCapacity < 0 )
        {
            throw new IllegalStateException( "Fail to increase queue capacity." );
        }
        long[] newValues = new long[newCapacity];
        int elementsFromHeadTillEnd = values.length - head;
        System.arraycopy( values, head, newValues, 0, elementsFromHeadTillEnd );
        System.arraycopy( values, 0, newValues, elementsFromHeadTillEnd, head );
        tail = values.length;
        head = 0;
        values = newValues;
    }

    private class PrimitiveLongArrayQueueIterator implements LongIterator
    {
        private int position;

        PrimitiveLongArrayQueueIterator()
        {
            this.position = head;
        }

        @Override
        public boolean hasNext()
        {
            return position != tail;
        }

        @Override
        public long next()
        {
            if ( hasNext() )
            {
                long value = values[position];
                position = (position + 1) & (values.length - 1);
                return value;
            }
            throw new NoSuchElementException();
        }
    }
}
