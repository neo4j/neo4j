/*
 * Copyright (c) 2002-2020 "Neo4j,"
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
import java.util.Set;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.neo4j.helpers.Format;
import org.neo4j.helpers.Listeners;
import org.neo4j.helpers.collection.Iterables;
import org.neo4j.logging.Log;

/**
 * Single database availability guard.
 *
 * @see AvailabilityGuard
 */
public class DatabaseAvailabilityGuard implements AvailabilityGuard
{
    private static final String DATABASE_AVAILABLE_MSG = "Fulfilling of requirement '%s' makes database %s available.";
    private static final String DATABASE_UNAVAILABLE_MSG = "Requirement `%s` makes database %s unavailable.";

    private final AtomicInteger requirementCount = new AtomicInteger( 0 );
    private final Set<AvailabilityRequirement> blockingRequirements = new CopyOnWriteArraySet<>();
    private final AtomicBoolean isShutdown = new AtomicBoolean( false );
    private final Listeners<AvailabilityListener> listeners = new Listeners<>();
    private final String databaseName;
    private final Clock clock;
    private final Log log;

    public DatabaseAvailabilityGuard( String databaseName, Clock clock, Log log )
    {
        this.databaseName = databaseName;
        this.clock = clock;
        this.log = log;
        this.listeners.add( new LoggingAvailabilityListener( log, databaseName ) );
    }

    @Override
    public void require( AvailabilityRequirement requirement )
    {
        if ( !blockingRequirements.add( requirement ) )
        {
            return;
        }

        synchronized ( requirementCount )
        {
            if ( requirementCount.getAndIncrement() == 0 && !isShutdown.get() )
            {
                log.info( DATABASE_UNAVAILABLE_MSG, requirement.description(), databaseName );
                listeners.notify( AvailabilityListener::unavailable );
            }
        }
    }

    @Override
    public void fulfill( AvailabilityRequirement requirement )
    {
        if ( !blockingRequirements.remove( requirement ) )
        {
            return;
        }

        synchronized ( requirementCount )
        {
            if ( requirementCount.getAndDecrement() == 1 && !isShutdown.get() )
            {
                log.info( DATABASE_AVAILABLE_MSG, requirement.description(), databaseName );
                listeners.notify( AvailabilityListener::available );
            }
        }
    }

    /**
     * Shutdown the guard. After this method is invoked, the database will always be considered unavailable.
     */
    public void shutdown()
    {
        synchronized ( requirementCount )
        {
            if ( isShutdown.getAndSet( true ) )
            {
                return;
            }

            if ( requirementCount.get() == 0 )
            {
                listeners.notify( AvailabilityListener::unavailable );
            }
        }
    }

    @Override
    public boolean isAvailable()
    {
        return availability() == Availability.AVAILABLE;
    }

    @Override
    public boolean isShutdown()
    {
        return availability() == Availability.SHUTDOWN;
    }

    @Override
    public boolean isAvailable( long millis )
    {
        return availability( millis ) == Availability.AVAILABLE;
    }

    @Override
    public void checkAvailable() throws UnavailableException
    {
        await( 0 );
    }

    @Override
    public void await( long millis ) throws UnavailableException
    {
        Availability availability = availability( millis );
        if ( availability == Availability.AVAILABLE )
        {
            return;
        }

        String description = (availability == Availability.UNAVAILABLE)
                ? "Timeout waiting for database to become available and allow new transactions. Waited " +
                Format.duration( millis ) + ". " + describeWhoIsBlocking()
                : "Database not available because it's shutting down";
        throw new UnavailableException( description );
    }

    private Availability availability()
    {
        if ( isShutdown.get() )
        {
            return Availability.SHUTDOWN;
        }

        int count = requirementCount.get();
        if ( count == 0 )
        {
            return Availability.AVAILABLE;
        }

        assert count > 0;

        return Availability.UNAVAILABLE;
    }

    private Availability availability( long millis )
    {
        Availability availability = availability();
        if ( availability != Availability.UNAVAILABLE )
        {
            return availability;
        }

        long timeout = clock.millis() + millis;
        do
        {
            try
            {
                Thread.sleep( 10 );
            }
            catch ( InterruptedException e )
            {
                Thread.interrupted();
                break;
            }
            availability = availability();
        } while ( availability == Availability.UNAVAILABLE && clock.millis() < timeout );

        return availability;
    }

    @Override
    public void addListener( AvailabilityListener listener )
    {
        listeners.add( listener );
    }

    @Override
    public void removeListener( AvailabilityListener listener )
    {
        listeners.remove( listener );
    }

    /**
     * @return a textual description of what components, if any, are blocking access
     */
    public String describeWhoIsBlocking()
    {
        if ( blockingRequirements.size() > 0 || requirementCount.get() > 0 )
        {
            String causes = Iterables.join( ", ", Iterables.map( AvailabilityRequirement::description, blockingRequirements ) );
            return requirementCount.get() + " reasons for blocking: " + causes + ".";
        }
        return "No blocking components";
    }

    private enum Availability
    {
        AVAILABLE,
        UNAVAILABLE,
        SHUTDOWN
    }

    private static class LoggingAvailabilityListener implements AvailabilityListener
    {
        private final Log log;
        private final String databaseName;

        LoggingAvailabilityListener( Log log, String databaseName )
        {
            this.log = log;
            this.databaseName = databaseName;
        }

        @Override
        public void available()
        {
            log.info( "Database %s is ready.", databaseName );
        }

        @Override
        public void unavailable()
        {
            log.info( "Database %s is unavailable.", databaseName );
        }
    }
}
