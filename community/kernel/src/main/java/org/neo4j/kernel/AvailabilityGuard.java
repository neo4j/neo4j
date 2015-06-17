/*
 * Copyright (c) 2002-2015 "Neo Technology,"
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

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;

import org.neo4j.helpers.Clock;
import org.neo4j.helpers.Format;
import org.neo4j.helpers.Function;
import org.neo4j.helpers.Listeners;
import org.neo4j.helpers.collection.Iterables;

import static org.neo4j.helpers.Listeners.notifyListeners;
import static org.neo4j.helpers.collection.Iterables.join;

/**
 * The availability guard is what ensures that the database will only take calls when it is in an ok state. It allows
 * query handling to easily determine if it is ok to call the database by calling {@link #isAvailable(long)}.
 * <p>
 * The implementation uses an atomic integer that is initialized to the nr of conditions that must be met for the
 * database to be available. Each such condition will then call grant/deny accordingly,
 * and if the integer becomes 0 access is granted.
 */
public class AvailabilityGuard
{
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
        };
    }

    private Iterable<AvailabilityListener> listeners = Listeners.newListeners();

    private final AtomicInteger available;
    private final List<AvailabilityRequirement> blockingComponents = new CopyOnWriteArrayList<>();
    private final Clock clock;

    public AvailabilityGuard( Clock clock )
    {
        this(clock, 0);
    }

    public AvailabilityGuard( Clock clock, int conditionCount )
    {
        this.clock = clock;
        available = new AtomicInteger( conditionCount );
    }

    public void deny( AvailabilityRequirement requirementNotMet )
    {
        int val;
        do
        {
            val = available.get();

            if ( val == -1 )
            {
                return;
            }

        } while ( !available.compareAndSet( val, val + 1 ) );

        blockingComponents.add( requirementNotMet );

        if ( val == 0 )
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

    public void grant( AvailabilityRequirement requirementNowMet )
    {
        int val;
        do
        {
            val = available.get();

            if ( val == -1 )
            {
                return;
            }

        } while ( !available.compareAndSet( val, val - 1 ) );

        assert available.get() >= 0;
        blockingComponents.remove( requirementNowMet );

        if ( val == 1 )
        {
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

    public void shutdown()
    {
        int val = available.getAndSet( -1 );
        if ( val == 0 )
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

    private static enum Availability
    {
        AVAILABLE( true, true ),
        TEMPORARILY_UNAVAILABLE( false, true ),
        UNAVAILABLE( false, false );

        private final boolean available;
        private final boolean temporarily;

        private Availability( boolean available, boolean temporarily )
        {
            this.available = available;
            this.temporarily = temporarily;
        }
    }

    /**
     * Determines if the database is available for transactions to use.
     *
     * @param millis to wait if not yet available.
     * @return true if it is available, otherwise false. Returns false immediately if shutdown.
     */
    public boolean isAvailable( long millis )
    {
        return availability( millis ).available;
    }

    private Availability availability( long millis )
    {
        int val = available.get();
        if ( val == 0 )
        {
            return Availability.AVAILABLE;
        }
        else if ( val == -1 )
        {
            return Availability.UNAVAILABLE;
        }
        else
        {
            long start = clock.currentTimeMillis();

            while ( clock.currentTimeMillis() < start + millis )
            {
                val = available.get();
                if ( val == 0 )
                {
                    return Availability.AVAILABLE;
                }
                else if ( val == -1 )
                {
                    return Availability.UNAVAILABLE;
                }

                try
                {
                    Thread.sleep( 10 );
                }
                catch ( InterruptedException e )
                {
                    Thread.interrupted();
                    break;
                }
                Thread.yield();
            }

            return Availability.TEMPORARILY_UNAVAILABLE;
        }
    }

    public <EXCEPTION extends Throwable> void checkAvailability( long millis, Class<EXCEPTION> cls )
            throws EXCEPTION
    {
        Availability availability = availability( millis );
        if ( !availability.available )
        {
            EXCEPTION exception;
            try
            {
                String description = availability.temporarily
                        ? "Timeout waiting for database to become available and allow new transactions. Waited " +
                                Format.duration( millis ) + ". " + describeWhoIsBlocking()
                        : "Database not available because it's shutting down";
                exception = cls.getConstructor( String.class ).newInstance( description );
            }
            catch ( NoSuchMethodException e )
            {
                throw new Error( "Bad exception class given to this method, it doesn't have a (String) constructor", e );
            }
            catch ( Exception e )
            {
                throw new RuntimeException( e );
            }
            throw exception;
        }
    }

    public void addListener( AvailabilityListener listener )
    {
        listeners = Listeners.addListener( listener, listeners );
    }

    public void removeListener( AvailabilityListener listener )
    {
        listeners = Listeners.removeListener( listener, listeners );
    }

    /** Provide a textual description of what components, if any, are blocking access. */
    public String describeWhoIsBlocking()
    {
        if(blockingComponents.size() > 0 || available.get() > 0)
        {
            String causes = join( ", ", Iterables.map( DESCRIPTION, blockingComponents ) );
            return available.get() + " reasons for blocking: " + causes + ".";
        }
        return "No blocking components";
    }

    public static final Function<AvailabilityRequirement,String> DESCRIPTION = new Function<AvailabilityRequirement,
            String>()
    {

        @Override
        public String apply( AvailabilityRequirement availabilityRequirement )
        {
            return availabilityRequirement.description();
        }
    };
}
