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
package org.neo4j.shell.kernel.apps;

import java.rmi.RemoteException;
import java.util.HashMap;
import java.util.Map;

import org.neo4j.cypher.CypherException;
import org.neo4j.cypher.javacompat.ExecutionEngine;
import org.neo4j.cypher.javacompat.ExecutionResult;
import org.neo4j.helpers.Service;
import org.neo4j.shell.App;
import org.neo4j.shell.AppCommandParser;
import org.neo4j.shell.Continuation;
import org.neo4j.shell.Output;
import org.neo4j.shell.Session;
import org.neo4j.shell.ShellException;

@Service.Implementation( App.class )
public class Start extends ReadOnlyGraphDatabaseApp
{
    public Start()
    {
        super();
    }

    @Override
    public String getDescription()
    {
        return "Executes a Cypher query. Usage: start <rest of query>;\n" +
                "Example: START me = node({self}) MATCH me-[:KNOWS]->you RETURN you.name;\n" +
                "where {self} will be replaced with the current location in the graph." +
                "Please, note that the query must end with a semicolon.";
    }

    @Override
    protected Continuation exec( AppCommandParser parser, Session session, Output out )
        throws ShellException, RemoteException
    {
        String query = parser.getLine();

        if ( isComplete(query) )
        {
            String queryWithoutSemicolon = query.substring(0, query.lastIndexOf(";"));

            ExecutionEngine engine = new ExecutionEngine( getServer().getDb() );
            try
            {
                ExecutionResult result = engine.execute( queryWithoutSemicolon, getParameters( session ) );
                out.println( result.toString() );
            }
            catch ( CypherException e )
            {
                throw ShellException.wrapCause( e );
            }
            return Continuation.INPUT_COMPLETE;
        }
        else
        {
            return Continuation.INPUT_INCOMPLETE;
        }
    }

    private Map<String, Object> getParameters( Session session ) throws ShellException
    {
        Map<String, Object> params = new HashMap<String, Object>();
        try
        {
            NodeOrRelationship self = getCurrent( session );
            params.put( "self", self.isNode() ? self.asNode() :self.asRelationship() );
        }
        catch ( ShellException e )
        { // OK, current didn't exist
        }
        return params;
    }

    private boolean isComplete(String query)
    {
        return query.trim().endsWith(";");
    }
}
