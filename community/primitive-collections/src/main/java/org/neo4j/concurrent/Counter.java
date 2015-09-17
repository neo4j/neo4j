/*
 * Copyright (c) 2002-2015 "Neo Technology,"
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
package org.neo4j.concurrent;

import java.util.concurrent.atomic.AtomicLong;

/**
 * Abstraction for the concept of a thread-safe counter.
 */
public final class Counter
{
    private final AtomicLong counter; // TODO 3.0 use a LongAdder when we move to Java 8.

    private Counter()
    {
        // No external instantiation.
        counter = new AtomicLong();
    }

    /**
     * Create a new Counter object, initialised to zero.
     * @return A new Counter.
     */
    public static Counter create()
    {
        return new Counter();
    }

    /**
     * Get the sum of all the increments and additions this counter has seen so far.
     * @return A long value.
     */
    public long sum()
    {
        return counter.get();
    }

    /**
     * Increment the value of the counter by 1.
     */
    public void increment()
    {
        add( 1 );
    }

    /**
     * Add the given delta to the counter, increasing its value.
     * @param value The amount to increase the count by.
     */
    public void add( long value )
    {
        counter.getAndAdd( value );
    }

    /**
     * Reset the value of the counter back to zero.
     */
    public void reset()
    {
        counter.set( 0 );
    }
}
