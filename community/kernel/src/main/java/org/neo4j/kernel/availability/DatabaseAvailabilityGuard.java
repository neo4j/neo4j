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
import java.util.Set;
import java.util.concurrent.CopyOnWriteArraySet;

import org.neo4j.graphdb.DatabaseShutdownException;
import org.neo4j.internal.helpers.Format;
import org.neo4j.internal.helpers.Listeners;
import org.neo4j.kernel.database.NamedDatabaseId;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;
import org.neo4j.logging.Log;

import static java.util.stream.Collectors.joining;

/**
 * Single database availability guard.
 *
 * @see AvailabilityGuard
 */
public class DatabaseAvailabilityGuard extends LifecycleAdapter implements AvailabilityGuard
{
    private static final String DATABASE_AVAILABLE_MSG = "Fulfilling of requirement '%s' makes database %s available.";
    private static final String DATABASE_UNAVAILABLE_MSG = "Requirement `%s` makes database %s unavailable.";

    private final Set<AvailabilityRequirement> blockingRequirements = new CopyOnWriteArraySet<>();
    private volatile boolean shutdown = true;
    private volatile Throwable startupFailure;
    private final Listeners<AvailabilityListener> listeners = new Listeners<>();
    private final NamedDatabaseId namedDatabaseId;
    private final Clock clock;
    private final Log log;
    private final long databaseTimeMillis;
    private final CompositeDatabaseAvailabilityGuard globalGuard;

    public DatabaseAvailabilityGuard( NamedDatabaseId namedDatabaseId, Clock clock, Log log, long databaseTimeMillis,
            CompositeDatabaseAvailabilityGuard globalGuard )
    {
        this.namedDatabaseId = namedDatabaseId;
        this.clock = clock;
        this.log = log;
        this.databaseTimeMillis = databaseTimeMillis;
        this.globalGuard = globalGuard;
        this.listeners.add( new LoggingAvailabilityListener( log, namedDatabaseId ) );
    }

    @Override
    public void init() throws Exception
    {
        shutdown = false;
        startupFailure = null;
    }

    @Override
    public void start() throws Exception
    {
        globalGuard.addDatabaseAvailabilityGuard( this );
    }

    @Override
    public void stop() throws Exception
    {
        globalGuard.removeDatabaseAvailabilityGuard( this );
    }

    @Override
    public void require( AvailabilityRequirement requirement )
    {
        if ( shutdown )
        {
            return;
        }
        if ( !blockingRequirements.add( requirement ) )
        {
            return;
        }

        if ( blockingRequirements.size() == 1 )
        {
            log.info( DATABASE_UNAVAILABLE_MSG, requirement.description(), namedDatabaseId.name() );
            listeners.notify( AvailabilityListener::unavailable );
        }
    }

    @Override
    public void fulfill( AvailabilityRequirement requirement )
    {
        if ( shutdown )
        {
            return;
        }
        if ( !blockingRequirements.remove( requirement ) )
        {
            return;
        }

        if ( blockingRequirements.isEmpty() )
        {
            log.info( DATABASE_AVAILABLE_MSG, requirement.description(), namedDatabaseId.name() );
            listeners.notify( AvailabilityListener::available );
        }
    }

    /**
     * If a database fails to start the exception can only be found in the debug log, which is very inconvenience in most, if not all cases.
     * This method allows database startup failure to tell this availability guard about that cause so that it can pass it to
     * the {@link DatabaseShutdownException} thrown from e.g. {@link #assertDatabaseAvailable()}.
     * @param cause cause of failure to start database.
     */
    public void startupFailure( Throwable cause )
    {
        startupFailure = cause;
    }

    /**
     * Shutdown the guard. After this method is invoked, the database will always be considered unavailable.
     */
    @Override
    public void shutdown()
    {
        shutdown = true;
        blockingRequirements.clear();
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

    public void assertDatabaseAvailable() throws UnavailableException
    {
        Availability availability = availability( databaseTimeMillis );
        switch ( availability )
        {
        case AVAILABLE:
            return;
        case SHUTDOWN:
            if ( startupFailure != null )
            {
                throw new DatabaseShutdownException( startupFailure );
            }
            throw new DatabaseShutdownException();
        case UNAVAILABLE:
            throwUnavailableException( databaseTimeMillis, availability );
        default:
            throw new IllegalStateException( "Unsupported availability mode: " + availability );
        }
    }

    @Override
    public void await( long millis ) throws UnavailableException
    {
        Availability availability = availability( millis );
        if ( availability == Availability.AVAILABLE )
        {
            return;
        }
        throwUnavailableException( millis, availability );
    }

    private void throwUnavailableException( long millis, Availability availability ) throws UnavailableException
    {
        String description = (availability == Availability.UNAVAILABLE)
                ? "Timeout waiting for database to become available and allow new transactions. Waited " +
                Format.duration( millis ) + ". " + describe()
                : "Database not available because it's shutting down";
        throw new UnavailableException( description );
    }

    private Availability availability()
    {
        if ( shutdown )
        {
            return Availability.SHUTDOWN;
        }
        return blockingRequirements.isEmpty() ? Availability.AVAILABLE : Availability.UNAVAILABLE;
    }

    private Availability availability( long millis )
    {
        Availability availability = availability();
        if ( availability == Availability.AVAILABLE )
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
        } while ( availability != Availability.AVAILABLE && clock.millis() < timeout );

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
    @Override
    public String describe()
    {
        Set<AvailabilityRequirement> requirementSet = this.blockingRequirements;
        int requirements = requirementSet.size();
        if ( requirements > 0 )
        {
            String causes = requirementSet.stream().map( AvailabilityRequirement::description ).collect( joining( ", " ) );
            return requirements + " reasons for blocking: " + causes + ".";
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
        private final NamedDatabaseId namedDatabaseId;

        LoggingAvailabilityListener( Log log, NamedDatabaseId namedDatabaseId )
        {
            this.log = log;
            this.namedDatabaseId = namedDatabaseId;
        }

        @Override
        public void available()
        {
            log.info( "Database %s is ready.", namedDatabaseId.name() );
        }

        @Override
        public void unavailable()
        {
            log.info( "Database %s is unavailable.", namedDatabaseId.name() );
        }
    }
}
