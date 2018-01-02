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
package org.neo4j.server.rest.management.console;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.ServiceLoader;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpSession;

import org.neo4j.helpers.collection.IteratorUtil;
import org.neo4j.logging.LogProvider;
import org.neo4j.server.database.CypherExecutor;
import org.neo4j.server.database.Database;
import org.neo4j.server.webadmin.console.ConsoleSessionCreator;
import org.neo4j.server.webadmin.console.ConsoleSessionFactory;
import org.neo4j.server.webadmin.console.ScriptSession;

public class SessionFactoryImpl implements ConsoleSessionFactory
{
    private static final Collection<ConsoleSessionCreator> creators = IteratorUtil.asCollection( ServiceLoader.load( ConsoleSessionCreator.class ) );

    private final HttpSession httpSession;
    private final CypherExecutor cypherExecutor;
    private final Map<String, ConsoleSessionCreator> engineCreators = new HashMap<>();
    private final HttpServletRequest request;

    public SessionFactoryImpl( HttpServletRequest request, List<String> supportedEngines, CypherExecutor cypherExecutor )
    {
        this.request = request;
        this.httpSession = request.getSession(true);
        this.cypherExecutor = cypherExecutor;

        enableEngines( supportedEngines );
    }

    @Override
    public ScriptSession createSession( String engineName, Database database, LogProvider logProvider )
    {
        engineName = engineName.toLowerCase();
        if ( engineCreators.containsKey( engineName ) )
        {
            return getOrInstantiateSession( database, engineName + "-console-session", engineCreators.get( engineName ), logProvider );
        }

        throw new IllegalArgumentException( "Unknown console engine '" + engineName + "'." );
    }

    @Override
    public Iterable<String> supportedEngines()
    {
        return engineCreators.keySet();
    }

    private ScriptSession getOrInstantiateSession( Database database, String key, ConsoleSessionCreator creator, LogProvider logProvider )
    {
        Object session = httpSession.getAttribute( key );
        if ( session == null )
        {
            session = creator.newSession( database, cypherExecutor, request, logProvider );
            httpSession.setAttribute( key, session );
        }
        return (ScriptSession) session;
    }

    private void enableEngines( List<String> supportedEngines )
    {
        for ( ConsoleSessionCreator creator : creators )
        {
            for ( String engineName : supportedEngines )
            {
                if ( creator.name().equalsIgnoreCase( engineName ) )
                {
                    engineCreators.put( engineName.toLowerCase(), creator );
                }
            }
        }
    }

}
