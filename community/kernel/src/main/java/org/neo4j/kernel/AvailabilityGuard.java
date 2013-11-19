/**
 * Copyright (c) 2002-2013 "Neo Technology,"
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

import java.util.concurrent.atomic.AtomicInteger;

import org.neo4j.helpers.Clock;
import org.neo4j.helpers.Listeners;

import static org.neo4j.helpers.Listeners.notifyListeners;

/**
 * The availability guard is what ensures that the database will only take calls when it is in an ok state. It allows
 * query handling to easily determine if it is ok to call the database by calling {@link #isAvailable(long)}.
 * <p/>
 * The implementation uses an atomic integer that is initialized to the nr of conditions that must be met for the
 * database to be available. Each such condition will then call grant/deny accordingly,
 * and if the integer becomes 0 access is granted.
 */
public class AvailabilityGuard
{
    private Iterable<AvailabilityListener> listeners = Listeners.newListeners();
    private final Clock clock;

    public interface AvailabilityListener
    {
        void available();

        void unavailable();
    }

    private final AtomicInteger available;

    public AvailabilityGuard( Clock clock, int conditionCount )
    {
        this.clock = clock;
        available = new AtomicInteger( conditionCount );
    }

    public void deny()
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

    public void grant()
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

    /**
     * Determines if the database is available for transactions to use.
     *
     * @param millis to wait if not yet available.
     * @return true if it is available, otherwise false. Returns false immediately if shutdown.
     */
    public boolean isAvailable( long millis )
    {
        int val = available.get();
        if ( val == 0 )
        {
            return true;
        }
        else if ( val == -1 )
        {
            return false;
        }
        else
        {
            long start = clock.currentTimeMillis();

            while ( clock.currentTimeMillis() < start + millis )
            {
                val = available.get();
                if ( val == 0 )
                {
                    return true;
                }
                else if ( val == -1 )
                {
                    return false;
                }


                Thread.yield();
            }

            return false;
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
}
