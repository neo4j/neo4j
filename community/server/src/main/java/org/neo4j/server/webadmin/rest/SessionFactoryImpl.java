/**
 * Copyright (c) 2002-2012 "Neo Technology,"
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
package org.neo4j.server.webadmin.rest;

import org.neo4j.server.database.CypherExecutor;
import org.neo4j.server.database.Database;
import org.neo4j.server.webadmin.console.CypherSession;
import org.neo4j.server.webadmin.console.GremlinSession;
import org.neo4j.server.webadmin.console.ScriptSession;

import javax.servlet.http.HttpSession;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class SessionFactoryImpl implements ConsoleSessionFactory
{
    private HttpSession httpSession;
    private final CypherExecutor cypherExecutor;
    private Map<String, ConsoleEngineCreator> engineCreators = new HashMap<String, ConsoleEngineCreator>();

    public SessionFactoryImpl( HttpSession httpSession, List<String> supportedEngines, CypherExecutor cypherExecutor )
    {
        this.httpSession = httpSession;
        this.cypherExecutor = cypherExecutor;

        enableEngines(supportedEngines);
    }

    @Override
    public ScriptSession createSession( String engineName, Database database )
    {
        engineName = engineName.toLowerCase();
        if(engineCreators.containsKey(engineName)) 
        {
            return getOrInstantiateSession( database, engineName + "-console-session", engineCreators.get(engineName));
        }
        
        throw new IllegalArgumentException("Unknown console engine '" + engineName + "'.");
    }

    @Override
    public Iterable<String> supportedEngines()
    {
        return engineCreators.keySet();
    }

    private ScriptSession getOrInstantiateSession( Database database, String key, ConsoleEngineCreator creator )
    {
        Object session = httpSession.getAttribute( key );
        if ( session == null )
        {
            session = creator.newSession( database, cypherExecutor );
            httpSession.setAttribute( key, session );
        }
        return (ScriptSession) session;
    }
    
    public static enum ConsoleEngineCreator
    {
        GREMLIN
        {
            @Override
            ScriptSession newSession( Database database, CypherExecutor cypherExecutor )
            {
                return new GremlinSession( database );
            }
        },
        CYPHER
        {
            @Override
            ScriptSession newSession( Database database , CypherExecutor cypherExecutor)
            {
                return new CypherSession( cypherExecutor );
            }
        },
        SHELL
        {
            @Override
            ScriptSession newSession( Database database, CypherExecutor cypherExecutor )
            {
                return new ShellSession( database.getGraph() );
            }
        };
        
        abstract ScriptSession newSession( Database database, CypherExecutor cypherExecutor );
    }


    private void enableEngines(List<String> supportedEngines)
    {
        for(String engineName : supportedEngines) 
        {
            for(ConsoleEngineCreator creator : EnumSet.allOf(ConsoleEngineCreator.class)) 
            {
                if(creator.name().equalsIgnoreCase(engineName)) 
                {
                    engineCreators.put(engineName.toLowerCase(), creator);
                }
            }
        }
    }

}
