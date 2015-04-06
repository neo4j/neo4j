/**
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
package org.neo4j.ndp.transport.http;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;

import org.neo4j.helpers.Clock;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;
import org.neo4j.ndp.runtime.Sessions;
import org.neo4j.ndp.runtime.Session;

import static org.neo4j.helpers.Clock.SYSTEM_CLOCK;

public class SessionRegistry extends LifecycleAdapter
{
    private final ConcurrentMap<String,SessionHolder> sessions = new ConcurrentHashMap<>();
    private final Sessions environment;
    private final Clock clock;

    public SessionRegistry( Sessions environment )
    {
        this(environment, SYSTEM_CLOCK);
    }

    public SessionRegistry( Sessions environment, Clock clock )
    {
        this.environment = environment;
        this.clock = clock;
    }

    public synchronized String create()
    {
        Session session = environment.newSession();
        sessions.put( session.key(), new SessionHolder( clock, session ) );
        return session.key();
    }

    public org.neo4j.ndp.transport.http.SessionAcquisition acquire( String key )
    {
        SessionHolder session = sessions.get( key );
        if ( session == null )
        {
            return org.neo4j.ndp.transport.http.SessionAcquisition.NOT_PRESENT;
        }

        if ( !session.acquire() )
        {
            return org.neo4j.ndp.transport.http.SessionAcquisition.NOT_AVAILABLE;
        }

        return session;
    }

    public void release( String key )
    {
        SessionHolder session = sessions.get( key );
        if ( session != null )
        {
            session.release();
        }
    }

    public void destroy( String key )
    {
        SessionHolder session = sessions.remove( key );
        if(session != null)
        {
            session.session().close();
        }
    }

    @Override
    public void stop() throws Throwable
    {
        destroyIdleSessions( 0, TimeUnit.SECONDS );
    }

    /**
     * Destroy all sessions that have been idle (as in the time since they were last actively in use) longer
     * than the given time interval.
     */
    public void destroyIdleSessions( long time, TimeUnit unit )
    {
        long deadline = clock.currentTimeMillis() - unit.toMillis( time );
        for ( String key : sessions.keySet() )
        {
            SessionHolder holder = sessions.get( key );
            if(holder.lastTimeActive() < deadline)
            {
                // Candidate, try and destroy it
                org.neo4j.ndp.transport.http.SessionAcquisition attempt = acquire( key );
                if(attempt.success())
                {
                    destroy( key );
                }
            }
        }

    }
}
