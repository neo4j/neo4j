/*
 * Copyright (c) 2002-2019 "Neo4j,"
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
package org.neo4j.test.rule;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;
import java.util.function.Predicate;

import org.neo4j.graphdb.TransientFailureException;
import org.neo4j.kernel.api.exceptions.Status;

/**
 * Retries a transaction a couple of times, with a delay in between.
 */
public class RetryACoupleOfTimesHandler implements RetryHandler
{
    private final Predicate<Throwable> retriable;
    private final int maxRetryCount;
    private final long timeBetweenTries;
    private final TimeUnit unit;
    private int retries;

    public RetryACoupleOfTimesHandler( Predicate<Throwable> retriable )
    {
        this( retriable, 5, 1, TimeUnit.SECONDS );
    }

    public RetryACoupleOfTimesHandler( Predicate<Throwable> retriable,
            int maxRetryCount, long timeBetweenTries, TimeUnit unit )
    {
        this.retriable = retriable;
        this.maxRetryCount = maxRetryCount;
        this.timeBetweenTries = timeBetweenTries;
        this.unit = unit;
    }

    @Override
    public boolean retryOn( Throwable t )
    {
        if ( retriable.test( t ) )
        {
            LockSupport.parkNanos( unit.toNanos( timeBetweenTries ) );
            return retries++ < maxRetryCount;
        }
        return false;
    }

    /**
     * Retries on {@link TransientFailureException} and any {@link Throwable} implementing
     * {@link org.neo4j.kernel.api.exceptions.Status.HasStatus} with
     * {@link org.neo4j.kernel.api.exceptions.Status.Classification#TransientError} classification.
     * a configurable number of times and with a configurable delay between retries.
     */
    public static final Predicate<Throwable> TRANSIENT_ERRORS = t ->
    {
        if ( t instanceof TransientFailureException )
        {
            return true;
        }
        if ( t instanceof Status.HasStatus )
        {
            Status status = ((Status.HasStatus) t).status();
            if ( status.code().classification() == Status.Classification.TransientError )
            {
                return true;
            }
        }
        return false;
    };

    /**
     * Retries on any {@link Exception}, i.e. not {@link OutOfMemoryError} or similar.
     */
    public static final Predicate<Throwable> ANY_EXCEPTION = t ->
    {
        return t instanceof Exception; // i.e. excluding OutOfMemory and more sever errors.
    };

    public static RetryHandler retryACoupleOfTimesOn( Predicate<Throwable> retriable )
    {
        return new RetryACoupleOfTimesHandler( retriable );
    }

    public static RetryHandler retryACoupleOfTimesOn( Predicate<Throwable> retriable,
            int maxRetryCount, long timeBetweenTries, TimeUnit unit )
    {
        return new RetryACoupleOfTimesHandler( retriable, maxRetryCount, timeBetweenTries, unit );
    }
}
