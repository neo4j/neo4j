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
package org.neo4j.kernel.availability;

import java.time.Clock;
import java.util.Collections;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArraySet;

import org.neo4j.kernel.lifecycle.LifecycleAdapter;
import org.neo4j.util.VisibleForTesting;

import static java.util.stream.Collectors.joining;

/**
 * Composite availability guard that makes decision about its availability based on multiple underlying database specific availability guards.
 * Any fulfillment, require, available, etc requests will be redistributed to all underlying availability guards.
 *
 * @see AvailabilityGuard
 */
public class CompositeDatabaseAvailabilityGuard extends LifecycleAdapter implements AvailabilityGuard
{
    private final Clock clock;
    private final CopyOnWriteArraySet<DatabaseAvailabilityGuard> guards = new CopyOnWriteArraySet<>();
    private volatile boolean started = true;

    public CompositeDatabaseAvailabilityGuard( Clock clock )
    {
        this.clock = clock;
    }

    void addDatabaseAvailabilityGuard( DatabaseAvailabilityGuard guard )
    {
        guards.add( guard );
    }

    void removeDatabaseAvailabilityGuard( DatabaseAvailabilityGuard guard )
    {
        guards.remove( guard );
    }

    @Override
    public void require( AvailabilityRequirement requirement )
    {
        guards.forEach( guard -> guard.require( requirement ) );
    }

    @Override
    public void fulfill( AvailabilityRequirement requirement )
    {
        guards.forEach( guard -> guard.fulfill( requirement ) );
    }

    @Override
    public void stop()
    {
        started = false;
    }

    @Override
    public boolean isAvailable()
    {
        return guards.stream().allMatch( DatabaseAvailabilityGuard::isAvailable ) && started;
    }

    @Override
    public boolean isShutdown()
    {
        return !started;
    }

    @Override
    public boolean isAvailable( long millis )
    {
        long totalWait = 0;
        for ( DatabaseAvailabilityGuard guard : guards )
        {
            long startMillis = clock.millis();
            if ( !guard.isAvailable( Math.max( 0, millis - totalWait ) ) )
            {
                return false;
            }
            totalWait += clock.millis() - startMillis;
            if ( totalWait > millis )
            {
                return false;
            }
        }
        return started;
    }

    @Override
    public void await( long millis ) throws UnavailableException
    {
        long totalWait = 0;
        for ( DatabaseAvailabilityGuard guard : guards )
        {
            long startMillis = clock.millis();
            guard.await( Math.max( 0, millis - totalWait ) );
            totalWait += clock.millis() - startMillis;
            if ( totalWait > millis )
            {
                throw new UnavailableException( getUnavailableMessage() );
            }
        }
        if ( !started )
        {
            throw new UnavailableException( getUnavailableMessage() );
        }
    }

    @Override
    public void addListener( AvailabilityListener listener )
    {
        throw new UnsupportedOperationException( "Composite guard does not support this operation." );
    }

    @Override
    public void removeListener( AvailabilityListener listener )
    {
        throw new UnsupportedOperationException( "Composite guard does not support this operation." );
    }

    @Override
    public String describe()
    {
        return guards.stream().map( DatabaseAvailabilityGuard::describe ).collect( joining( ", " ) );
    }

    @VisibleForTesting
    public Set<DatabaseAvailabilityGuard> getGuards()
    {
        return Collections.unmodifiableSet( guards );
    }

    private String getUnavailableMessage()
    {
        return "Database is not available: " + describe();
    }
}
