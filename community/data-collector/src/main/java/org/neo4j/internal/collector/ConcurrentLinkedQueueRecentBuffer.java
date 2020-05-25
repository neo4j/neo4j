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
package org.neo4j.internal.collector;

import org.apache.commons.lang3.mutable.MutableInt;

import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.function.Predicate;

/**
 * Implementation of {@link RecentBuffer} using {@link ConcurrentLinkedQueue}.
 */
public class ConcurrentLinkedQueueRecentBuffer<T> implements RecentBuffer<T>
{
    private final ConcurrentLinkedQueue<T> queue;
    private final int maxSize;
    private final AtomicInteger size;

    public ConcurrentLinkedQueueRecentBuffer( int maxSize )
    {
        this.maxSize = maxSize;
        queue = new ConcurrentLinkedQueue<>();
        size = new AtomicInteger( 0 );
    }

    /* ---- many producers ---- */

    @Override
    public void produce( T t )
    {
        queue.add( t );
        int newSize = size.incrementAndGet();
        if ( newSize > maxSize )
        {
            queue.poll();
            size.decrementAndGet();
        }
    }

    /* ---- single consumer ---- */

    @Override
    public void clearIf( Predicate<T> predicate )
    {
        var removeCount = new MutableInt( 0 );
        queue.removeIf( q ->
                        {
                            if ( predicate.test( q ) )
                            {
                                removeCount.increment();
                                return true;
                            }
                            else
                            {
                                return false;
                            }
                        } );
        // Might go out of sync with queue here, but should be minor slippage.
        // Will not accumulate leaks either, but reset on every clear.
        size.addAndGet( -removeCount.intValue() );
    }

    @Override
    public void foreach( Consumer<T> consumer )
    {
        queue.forEach( consumer );
    }
}
