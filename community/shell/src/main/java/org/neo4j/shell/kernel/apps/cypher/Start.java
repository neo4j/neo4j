/**
 * Copyright (c) 2002-2013 "Neo Technology,"
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

import java.io.PrintWriter;
import java.rmi.RemoteException;
import java.util.Map;

import org.neo4j.cypher.CypherException;
import org.neo4j.cypher.javacompat.ExecutionEngine;
import org.neo4j.cypher.javacompat.ExecutionResult;
import org.neo4j.graphdb.DependencyResolver;
import org.neo4j.helpers.Service;
import org.neo4j.kernel.impl.util.StringLogger;
import org.neo4j.kernel.logging.Logging;
import org.neo4j.shell.App;
import org.neo4j.shell.AppCommandParser;
import org.neo4j.shell.Continuation;
import org.neo4j.shell.Output;
import org.neo4j.shell.OutputAsWriter;
import org.neo4j.shell.Session;
import org.neo4j.shell.ShellException;
import org.neo4j.shell.kernel.apps.NodeOrRelationship;
import org.neo4j.shell.kernel.apps.TransactionProvidingApp;

@Service.Implementation(App.class)
public class Start extends TransactionProvidingApp
{
    private ExecutionEngine engine;

    @Override
    public String getDescription()
    {
        return "Executes a Cypher query. Usage: start <rest of query>;\n" +
                "Example: START me = node({self}) MATCH me-[:KNOWS]->you RETURN you.name;\n" +
                "where {self} will be replaced with the current location in the graph." +
                "Please, note that the query must end with a semicolon. Other parameters are\n" +
                "taken from shell variables, see 'help export'.";
    }

    @Override
    protected Continuation exec( AppCommandParser parser, Session session, Output out )
            throws ShellException, RemoteException
    {
        String query = parser.getLine().trim();

        if ( isComplete( query ) )
        {

            try
            {
                final long startTime = System.currentTimeMillis();
                ExecutionResult result = getEngine().execute( trimQuery( query ), getParameters( session ) );
                handleResult( out, result, startTime, session, parser );
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

    protected String trimQuery( String query )
    {
        return query.substring( 0, query.lastIndexOf( ";" ) );
    }

    protected void handleResult( Output out, ExecutionResult result, long startTime, Session session,
            AppCommandParser parser ) throws RemoteException, ShellException
    {
        printResult( out, result, startTime );
    }

    private void printResult( Output out, ExecutionResult result, long startTime ) throws RemoteException
    {
        result.toString( new PrintWriter( new OutputAsWriter( out ) ) );
        out.println( (now() - startTime) + " ms" );
    }

    protected StringLogger getCypherLogger()
    {
        DependencyResolver dependencyResolver = getServer().getDb().getDependencyResolver();
        Logging logging = dependencyResolver.resolveDependency( Logging.class );
        return logging.getMessagesLog( ExecutionEngine.class );
    }

    protected Map<String, Object> getParameters(Session session) throws ShellException
    {
        try
        {
            NodeOrRelationship self = getCurrent( session );
            session.set( "self", self.isNode() ? self.asNode() : self.asRelationship() );
        }
        catch ( ShellException e )
        { // OK, current didn't exist
        }
        return session.asMap();
    }

    protected boolean isComplete( String query )
    {
        return query.endsWith( ";" );
    }


    protected ExecutionEngine getEngine()
    {
        if ( this.engine == null )
        {
            synchronized ( this )
            {
                if ( this.engine == null )
                {
                    this.engine = new ExecutionEngine( getServer().getDb(), getCypherLogger() );
                }
            }
        }
        return this.engine;
    }

    protected long now()
    {
        return System.currentTimeMillis();
    }
}
