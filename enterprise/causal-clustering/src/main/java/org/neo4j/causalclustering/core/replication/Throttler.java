/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * GNU AFFERO GENERAL PUBLIC LICENSE Version 3
 * (http://www.fsf.org/licensing/licenses/agpl-3.0.html) with the
 * Commons Clause, as found in the associated LICENSE.txt file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * Neo4j object code can be licensed independently from the source
 * under separate terms from the AGPL. Inquiries can be directed to:
 * licensing@neo4j.com
 *
 * More information is also available at:
 * https://neo4j.com/licensing/
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
