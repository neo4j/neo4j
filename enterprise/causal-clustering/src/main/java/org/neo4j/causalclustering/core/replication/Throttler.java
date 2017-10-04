/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
package org.neo4j.causalclustering.core.replication;

import org.neo4j.function.ThrowingSupplier;

/**
 * Throttles calls to underlying invocation based on available credits.
 */
class Throttler
{
    private final long creditLimit;
    private long currentCredit;

    Throttler( long creditLimit )
    {
        this.creditLimit = creditLimit;
    }

    private synchronized void acquire( long credits ) throws InterruptedException
    {
        while ( currentCredit >= creditLimit )
        {
            wait();
        }

        currentCredit += credits;

        if ( currentCredit < creditLimit )
        {
            notify();
        }
    }

    private synchronized void release( long credits )
    {
        currentCredit -= credits;

        if ( currentCredit < creditLimit )
        {
            notify();
        }
    }

    <R, E extends Exception> R invoke( ThrowingSupplier<R,E> call, long credits ) throws E, InterruptedException
    {
        acquire( credits );
        try
        {
            return call.get();
        }
        finally
        {
            release( credits );
        }
    }
}
