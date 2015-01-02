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
package org.neo4j.shell.kernel.apps.cypher;

import java.rmi.RemoteException;

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
public class Profile extends Start
{
    public Profile()
    {
        super();
    }

    @Override
    public String getDescription()
    {
        return "Executes a Cypher query and prints out execution plan and other profiling information. " +
                "Usage: profile <query>\n" +
                "Example: PROFILE START me = node({self}) MATCH me-[:KNOWS]->you RETURN you.name\n" +
                "where {self} will be replaced with the current location in the graph";
    }

    @Override
    protected Continuation exec( AppCommandParser parser, Session session, Output out )
            throws ShellException, RemoteException
    {
        String query = parser.getLineWithoutApp();

        if ( isComplete( query ) )
        {
            ExecutionEngine engine = new ExecutionEngine( getServer().getDb(), getCypherLogger() );
            try
            {
                ExecutionResult result = engine.profile( query, getParameters( session ) );
                out.println( result.dumpToString() );
                out.println( result.executionPlanDescription().toString() );
            } catch ( CypherException e )
            {
                throw ShellException.wrapCause( e );
            }
            return Continuation.INPUT_COMPLETE;
        } else
        {
            return Continuation.INPUT_INCOMPLETE;
        }
    }
}
