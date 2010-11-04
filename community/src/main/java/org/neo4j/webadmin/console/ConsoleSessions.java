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

package org.neo4j.webadmin.console;

import java.util.Collection;
import java.util.Iterator;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Keeps track of currently running gremlin sessions. Each one is associated
 * with a web client.
 * 
 * @author Jacob Hansson <jacob@voltvoodoo.com>
 * 
 */
public class ConsoleSessions
{

    protected static ConcurrentHashMap<String, ConsoleSession> sessions = new ConcurrentHashMap<String, ConsoleSession>();

    //
    // PUBLIC
    //

    /**
     * Gets a GremlinSesssion for a given sessionId, creating a GremlinSession
     * if one does not exist.
     */
    public static ConsoleSession getSession( String sessionId )
    {
        ensureSessionExists( sessionId );
        return sessions.get( sessionId );
    }

    public static void destroySession( String sessionId )
    {
        sessions.get( sessionId ).die();
        sessions.remove( sessionId );
    }

    public static void destroyAllSessions()
    {
        Iterator<String> keys = sessions.keySet().iterator();
        while ( keys.hasNext() )
        {
            destroySession( keys.next() );
        }
    }

    public static boolean hasSession( String sessionId )
    {
        return sessions.containsKey( sessionId );
    }

    public static Collection<String> getSessionIds()
    {
        return sessions.keySet();
    }

    //
    // INTERNALS
    //

    protected static void ensureSessionExists( String sessionId )
    {
        if ( !sessions.containsKey( sessionId ) )
        {
            sessions.put( sessionId, new ConsoleSession() );
        }
    }

}
