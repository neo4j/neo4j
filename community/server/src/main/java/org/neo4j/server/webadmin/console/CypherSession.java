/**
 * Copyright (c) 2002-2014 "Neo Technology,"
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
/**
 * Copyright (c) 2002-2011 "Neo Technology,"
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
package org.neo4j.server.webadmin.console;

import org.neo4j.cypher.SyntaxException;
import org.neo4j.cypher.javacompat.ExecutionResult;
import org.neo4j.helpers.Pair;
import org.neo4j.server.database.CypherExecutor;
import org.neo4j.server.logging.Logger;

public class CypherSession implements ScriptSession
{
    private final CypherExecutor cypherExecutor;
    private static Logger log = Logger.getLogger( CypherSession.class );

    public CypherSession( CypherExecutor cypherExecutor )
    {
        this.cypherExecutor = cypherExecutor;
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
            ExecutionResult result = cypherExecutor.getExecutionEngine().execute( script );
            resultString = result.dumpToString();
        }
        catch ( SyntaxException error )
        {
            resultString = error.getMessage();
        }
        catch ( Exception exception )
        {
            log.error( exception );
            resultString = "Error: " + exception.getClass().getSimpleName() + " - " + exception.getMessage();
        }
        return Pair.of( resultString, null );
    }
}
