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
package org.neo4j.test.rule;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;

import org.neo4j.graphdb.TransientFailureException;

/**
 * Retries on {@link TransientFailureException} a configurable number of times and with a configurable
 * delay between retries.
 */
public class RetryOnTransientFailure implements RetryHandler
{
    private final int maxRetryCount;
    private final long timeBetweenTries;
    private final TimeUnit unit;
    private int retries;

    public RetryOnTransientFailure()
    {
        this( 5, 1, TimeUnit.SECONDS );
    }

    public RetryOnTransientFailure( int maxRetryCount, long timeBetweenTries, TimeUnit unit )
    {
        this.maxRetryCount = maxRetryCount;
        this.timeBetweenTries = timeBetweenTries;
        this.unit = unit;
    }

    @Override
    public boolean retryOn( Throwable t )
    {
        if ( t instanceof TransientFailureException )
        {
            LockSupport.parkNanos( unit.toNanos( timeBetweenTries ) );
            return retries++ < maxRetryCount;
        }
        return false;
    }
}
