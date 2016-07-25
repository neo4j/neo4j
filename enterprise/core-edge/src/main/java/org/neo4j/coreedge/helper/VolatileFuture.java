/*
 * Copyright (c) 2002-2016 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.coreedge.helper;

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
        if( this.value != value )
        {
            this.value = value;
            notifyAll();
        }
    }

    public T get( long timeoutMillis, Predicate<T> predicate ) throws TimeoutException, InterruptedException
    {
        T alias = value;
        if( predicate.test( alias ) )
        {
            return alias;
        }

        if( timeoutMillis == 0 )
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
