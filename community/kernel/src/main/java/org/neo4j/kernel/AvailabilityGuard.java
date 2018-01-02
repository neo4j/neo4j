/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.kernel;

import java.util.Set;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.neo4j.function.Function;
import org.neo4j.helpers.Clock;
import org.neo4j.helpers.Format;
import org.neo4j.helpers.Listeners;
import org.neo4j.helpers.collection.Iterables;
import org.neo4j.logging.Log;

import static org.neo4j.helpers.Listeners.notifyListeners;
import static org.neo4j.helpers.collection.Iterables.join;

/**
 * The availability guard ensures that the database will only take calls when it is in an ok state.
 * It tracks a set of requirements (added via {@link #require(AvailabilityRequirement)}) that must all be marked
 * as fulfilled (using {@link #fulfill(AvailabilityRequirement)}) before the database is considered available again.
 * Consumers determine if it is ok to call the database using {@link #isAvailable()},
 * or await availability using {@link #isAvailable(long)}.
 */
public class AvailabilityGuard
{

    public static final String DATABASE_AVAILABLE_MSG = "Fulfilling of requirement makes database available: ";
    public static final String DATABASE_UNAVAILABLE_MSG = "Requirement makes database unavailable: ";

    public class UnavailableException extends Exception
    {
        public UnavailableException( String message )
        {
            super( message );
        }
    }

    public interface AvailabilityListener
    {
        void available();

        void unavailable();
    }

    /**
     * Represents a description of why someone is denying access to the database, to help debugging. Components
     * granting and revoking access should use the same denial reason for both method calls, as it is used to track
     * who is blocking access to the database.
     */
    public interface AvailabilityRequirement
    {
        String description();
    }

    public static AvailabilityRequirement availabilityRequirement( final String descriptionWhenBlocking )
    {
        return new AvailabilityRequirement()
        {
            @Override
            public String description()
            {
                return descriptionWhenBlocking;
            }

            @Override
            public boolean equals( Object o )
            {
                if ( this == o )
                {
                    return true;
                }
                if ( o == null || getClass() != o.getClass() )
                {
                    return false;
                }

                AvailabilityRequirement that = (AvailabilityRequirement) o;

                return descriptionWhenBlocking == null ?
                        that.description() == null :
                        descriptionWhenBlocking.equals( that.description() );
            }

            @Override
            public int hashCode()
            {
                return descriptionWhenBlocking != null ? descriptionWhenBlocking.hashCode() : 0;
            }
        };
    }

    private final AtomicInteger requirementCount = new AtomicInteger( 0 );
    private final Set<AvailabilityRequirement> blockingRequirements = new CopyOnWriteArraySet<>();
    private final AtomicBoolean isShutdown = new AtomicBoolean( false );
    private Iterable<AvailabilityListener> listeners = Listeners.newListeners();
    private final Clock clock;
    private final Log log;

    public AvailabilityGuard( Clock clock, Log log )
    {
        this.clock = clock;
        this.log = log;
    }

    /**
     * Indicate a requirement that must be fulfilled before the database is considered available.
     *
     * @param requirement the requirement object
     */
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
                log.info( DATABASE_UNAVAILABLE_MSG + requirement.description() );
                notifyListeners( listeners, new Listeners.Notification<AvailabilityListener>()
                {
                    @Override
                    public void notify( AvailabilityListener listener )
                    {
                        listener.unavailable();
                    }
                } );
            }
        }
    }

    /**
     * Indicate that a requirement has been fulfilled.
     *
     * @param requirement the requirement object
     */
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
                log.info( DATABASE_AVAILABLE_MSG + requirement.description() );
                notifyListeners( listeners, new Listeners.Notification<AvailabilityListener>()
                {
                    @Override
                    public void notify( AvailabilityListener listener )
                    {
                        listener.available();
                    }
                } );
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
                notifyListeners( listeners, new Listeners.Notification<AvailabilityListener>()
                {
                    @Override
                    public void notify( AvailabilityListener listener )
                    {
                        listener.unavailable();
                    }
                } );
            }
        }
    }

    private static enum Availability
    {
        AVAILABLE,
        UNAVAILABLE,
        SHUTDOWN
    }

    /**
     * Check if the database is available for transactions to use.
     *
     * @return true if there are no requirements waiting to be fulfilled and the guard has not been shutdown
     */
    public boolean isAvailable()
    {
        return availability() == Availability.AVAILABLE;
    }

    /**
     * Check if the database is available for transactions to use.
     *
     * @param millis to wait for availability
     * @return true if there are no requirements waiting to be fulfilled and the guard has not been shutdown
     */
    public boolean isAvailable( long millis )
    {
        return availability( millis ) == Availability.AVAILABLE;
    }

    /**
     * Checks if available. If not then an {@link UnavailableException} is thrown describing why.
     * This methods doesn't wait like {@link #await(long)} does.
     *
     * @throws UnavailableException if not available.
     */
    public void checkAvailable() throws UnavailableException
    {
        await( 0 );
    }

    /**
     * Await the database becoming available.
     *
     * @param millis to wait for availability
     * @throws UnavailableException thrown when the timeout has been exceeded or the guard has been shutdown
     */
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

        assert (count > 0);

        return Availability.UNAVAILABLE;
    }

    private Availability availability( long millis )
    {
        Availability availability = availability();
        if ( availability != Availability.UNAVAILABLE )
        {
            return availability;
        }

        long timeout = clock.currentTimeMillis() + millis;
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
        } while ( availability == Availability.UNAVAILABLE && clock.currentTimeMillis() < timeout );

        return availability;
    }

    /**
     * Add a listener for changes to availability.
     *
     * @param listener the listener to receive callbacks when availability changes
     */
    public void addListener( AvailabilityListener listener )
    {
        listeners = Listeners.addListener( listener, listeners );
    }

    /**
     * Remove a listener for changes to availability.
     *
     * @param listener the listener to remove
     */
    public void removeListener( AvailabilityListener listener )
    {
        listeners = Listeners.removeListener( listener, listeners );
    }

    /**
     * @return a textual description of what components, if any, are blocking access
     */
    public String describeWhoIsBlocking()
    {
        if ( blockingRequirements.size() > 0 || requirementCount.get() > 0 )
        {
            String causes = join( ", ", Iterables.map( DESCRIPTION, blockingRequirements ) );
            return requirementCount.get() + " reasons for blocking: " + causes + ".";
        }
        return "No blocking components";
    }

    public static final Function<AvailabilityRequirement, String> DESCRIPTION =
            new Function<AvailabilityRequirement, String>()
            {
                @Override
                public String apply( AvailabilityRequirement availabilityRequirement )
                {
                    return availabilityRequirement.description();
                }
            };
}
