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

import java.util.Collections;
import javax.servlet.http.HttpServletRequest;

import org.neo4j.cypher.SyntaxException;
import org.neo4j.graphdb.Result;
import org.neo4j.helpers.Pair;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;
import org.neo4j.server.database.CypherExecutor;
import org.neo4j.server.rest.web.ServerQuerySession;
import org.neo4j.server.webadmin.console.ScriptSession;

public class CypherSession implements ScriptSession
{
    private final CypherExecutor cypherExecutor;
    private final Log log;
    private final HttpServletRequest request;

    public CypherSession( CypherExecutor cypherExecutor, LogProvider logProvider, HttpServletRequest request )
    {
        this.cypherExecutor = cypherExecutor;
        this.log = logProvider.getLog( getClass() );
        this.request = request;
    }

    @Override
    public Pair<String, String> evaluate( String script )
    {
        if ( script.trim().equals( "" ) )
        {
            return Pair.of( "", null );
        }

        String resultString;
        try
        {
            Result result = cypherExecutor.getExecutionEngine().executeQuery( script, Collections.<String, Object>emptyMap(), new ServerQuerySession( request ) );
            resultString = result.resultAsString();
        }
        catch ( SyntaxException error )
        {
            resultString = error.getMessage();
        }
        catch ( Exception exception )
        {
            log.error( "Unknown error executing cypher query", exception );
            resultString = "Error: " + exception.getClass().getSimpleName() + " - " + exception.getMessage();
        }
        return Pair.of( resultString, null );
    }
}
