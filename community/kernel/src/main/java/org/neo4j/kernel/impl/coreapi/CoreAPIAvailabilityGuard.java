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
package org.neo4j.kernel.impl.coreapi;

import org.neo4j.graphdb.DatabaseShutdownException;
import org.neo4j.kernel.AvailabilityGuard;

/**
 * This is a simple wrapper around {@link AvailabilityGuard} that augments its behavior to match how
 * availability errors and timeouts are handled in the Core API.
 */
public class CoreAPIAvailabilityGuard
{
    private final AvailabilityGuard guard;
    private final long timeout;

    public CoreAPIAvailabilityGuard( AvailabilityGuard guard, long timeout )
    {
        this.guard = guard;
        this.timeout = timeout;
    }

    public boolean isAvailable( long timeoutMillis )
    {
        return guard.isAvailable( timeoutMillis );
    }

    public void assertDatabaseAvailable()
    {
        try
        {
            guard.await( timeout );
        }
        catch ( AvailabilityGuard.UnavailableException e )
        {
            if ( guard.isShutdown() )
            {
                throw new DatabaseShutdownException();
            }
            throw new org.neo4j.graphdb.TransactionFailureException( e.getMessage(), e );
        }
    }
}
