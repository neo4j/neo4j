/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * Neo4j Sweden Software License, as found in the associated LICENSE.txt
 * file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * Neo4j Sweden Software License for more details.
 */
package org.neo4j.causalclustering.helper;

import java.util.concurrent.TimeoutException;
import java.util.function.Predicate;

/**
 * For synchronising using wait/notify on a volatile value.
 */
public class VolatileFuture<T>
{
    private volatile T value;

    public VolatileFuture( T initialValue )
    {
        this.value = initialValue;
    }

    public synchronized void set( T value )
    {
        if ( this.value != value )
        {
            this.value = value;
            notifyAll();
        }
    }

    public T get( long timeoutMillis, Predicate<T> predicate ) throws TimeoutException, InterruptedException
    {
        T alias = value;
        if ( predicate.test( alias ) )
        {
            return alias;
        }

        if ( timeoutMillis == 0 )
        {
            throw new TimeoutException();
        }

        return waitForValue( timeoutMillis + System.currentTimeMillis(), predicate );
    }

    private synchronized T waitForValue( long endTimeMillis, Predicate<T> predicate ) throws InterruptedException, TimeoutException
    {
        T alias;
        while ( !predicate.test( alias = value ) )
        {
            long timeLeft = endTimeMillis - System.currentTimeMillis();
            if ( timeLeft > 0 )
            {
                wait( timeLeft );
            }
            else
            {
                throw new TimeoutException();
            }
        }

        return alias;
    }
}
