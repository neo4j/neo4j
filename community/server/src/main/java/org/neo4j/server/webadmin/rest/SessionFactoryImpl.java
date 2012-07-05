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

import javax.servlet.http.HttpSession;

import org.neo4j.server.database.Database;
import org.neo4j.server.webadmin.console.CypherSession;
import org.neo4j.server.webadmin.console.GremlinSession;
import org.neo4j.server.webadmin.console.ScriptSession;

public class SessionFactoryImpl implements SessionFactory
{
    private HttpSession httpSession;

    public SessionFactoryImpl( HttpSession httpSession )
    {
        this.httpSession = httpSession;
    }

    @Override
    public ScriptSession createSession( String engineName, Database database )
    {
        if ( engineName.equals( "shell" ) )
        {
//            return new CypherSession( database.graph );
            return getOrInstantiateSession( database, "shellSession", SessionCreator.SHELL );
        }
        else
        {
            return getOrInstantiateSession( database, "consoleSession", SessionCreator.GREMLIN );
        }
    }

    private ScriptSession getOrInstantiateSession( Database database, String key, SessionCreator creator )
    {
        Object session = httpSession.getAttribute( key );
        if ( session == null )
        {
            session = creator.newSession( database );
            httpSession.setAttribute( key, session );
        }
        return (ScriptSession) session;
    }
    
    private static enum SessionCreator
    {
        GREMLIN
        {
            @Override
            ScriptSession newSession( Database database )
            {
                return new GremlinSession( database );
            }
        },
        CYPHER
        {
            @Override
            ScriptSession newSession( Database database )
            {
                return new CypherSession( database.getGraph() );
            }
        },
        SHELL
        {
            @Override
            ScriptSession newSession( Database database )
            {
                return new ShellSession( database.getGraph() );
            }
        };
        
        abstract ScriptSession newSession( Database database );
    }
}
