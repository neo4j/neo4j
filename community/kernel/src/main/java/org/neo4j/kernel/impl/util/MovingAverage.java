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
package org.neo4j.kernel.impl.util;

import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicLongArray;

import static java.lang.Math.min;

/**
 * In a moving average calculation, only the last N values are considered.
 */
public class MovingAverage
{
    private final AtomicLongArray values;
    private final AtomicLong total = new AtomicLong();
    private final AtomicLong valueCursor = new AtomicLong();

    public MovingAverage( int numberOfTrackedValues )
    {
        this.values = new AtomicLongArray( numberOfTrackedValues );
    }

    public void add( long value )
    {
        long cursor = valueCursor.getAndIncrement();
        long prevValue = values.getAndSet( (int) (cursor % values.length()), value );
        total.addAndGet( value - prevValue );
    }

    private int numberOfCurrentlyTrackedValues()
    {
        return (int) min( valueCursor.get(), values.length() );
    }

    public long total()
    {
        return total.get();
    }

    public long average()
    {
        int trackedValues = numberOfCurrentlyTrackedValues();
        return trackedValues > 0 ? total.get() / trackedValues : 0;
    }

    public void reset()
    {
        for ( int i = 0; i < values.length(); i++ )
        {
            values.set( i, 0 );
        }
        total.set( 0 );
        valueCursor.set( 0 );
    }
}
