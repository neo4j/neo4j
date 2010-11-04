/**
 * Copyright (c) 2002-2010 "Neo Technology,"
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

package org.neo4j.server.webadmin.console;

import java.util.Collection;

/**
 * Remove gremlin sessions that have been idle for too long.
 * 
 * Based on Webling garbage collector by Pavel A. Yaskevich.
 * 
 * @author Jacob Hansson <jacob@voltvoodoo.com>
 * 
 */
public class ConsoleGarbageCollector extends Thread
{

    long updateInterval = 3000000; // 50 minutes
    long maxIdleInterval = 1790000; // 29 minutes

    ConsoleGarbageCollector()
    {
        setDaemon( true );
        start();
    }

    @Override
    public void run()
    {

        while ( true )
        {
            try
            {
                Thread.sleep( updateInterval );
            }
            catch ( InterruptedException e )
            {
            }

            Collection<String> sessionIds = ConsoleSessions.getSessionIds();

            for ( String sessionId : sessionIds )
            {
                // Make sure session exists (otherwise
                // GremlinSessions.getSession() will create it)
                if ( ConsoleSessions.hasSession( sessionId ) )
                {
                    // If idle time is above our threshold
                    if ( ConsoleSessions.getSession( sessionId ).getIdleTime() > maxIdleInterval )
                    {
                        // Throw the GremlinSession instance to the wolves
                        ConsoleSessions.destroySession( sessionId );
                    }
                }
            }
        }
    }

}
