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
package org.neo4j.shell.kernel.apps.cypher;

import java.io.PrintWriter;
import java.rmi.RemoteException;
import java.text.MessageFormat;
import java.util.Map;

import org.neo4j.graphdb.Result;
import org.neo4j.helpers.Service;
import org.neo4j.kernel.impl.query.QueryExecutionEngine;
import org.neo4j.kernel.impl.query.QuerySession;
import org.neo4j.kernel.impl.query.QueryExecutionKernelException;
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
    private QueryExecutionEngine engine;

    @Override
    public String getDescription()
    {
        String className = this.getClass().getSimpleName().toUpperCase();
        return MessageFormat.format( "Executes a Cypher query. Usage: {0} <rest of query>;\nExample: MATCH " +
                "(me)-[:KNOWS]->(you) RETURN you.name;\nwhere '{'self'}' will be replaced with the current location in " +
                "the graph.Please, note that the query must end with a semicolon. Other parameters are\ntaken from " +
                "shell variables, see ''help export''.", className );
    }

    @Override
    protected Continuation exec( AppCommandParser parser, Session session, Output out )
            throws ShellException, RemoteException
    {
        String query = parser.getLine().trim();

        if ( isComplete( query ) )
        {
            final long startTime = System.currentTimeMillis();
            try
            {
                Result result = getResult( trimQuery( query ), session );
                handleResult( out, result, startTime );
            }
            catch ( QueryExecutionKernelException e )
            {
                handleException( out, e, startTime );
                return Continuation.EXCEPTION_CAUGHT;
            }
            return Continuation.INPUT_COMPLETE;
        }
        else
        {
            return Continuation.INPUT_INCOMPLETE;
        }
    }

    protected Result getResult( String query, Session session )
            throws ShellException, RemoteException, QueryExecutionKernelException
    {
        return getEngine().executeQuery( query, getParameters( session ), shellSession( session ) );
    }

    protected String trimQuery( String query )
    {
        return query.substring( 0, query.lastIndexOf( ";" ) );
    }

    protected void handleResult( Output out, Result result, long startTime )
            throws RemoteException, ShellException
    {
        printResult( out, result, startTime );
    }

    private void printResult( Output out, Result result, long startTime ) throws RemoteException
    {
        result.writeAsStringTo( new PrintWriter( new OutputAsWriter( out ) ) );
        out.println( (now() - startTime) + " ms" );
        if ( result.getQueryExecutionType().requestedExecutionPlanDescription() )
        {
            out.println();
            out.println( result.getExecutionPlanDescription().toString() );
        }
    }

    protected void handleException( Output out, QueryExecutionKernelException exception, long startTime )
            throws RemoteException
    {
        out.println( (now() - startTime) + " ms" );
        out.println();
        out.println("WARNING: " + exception.getMessage());
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


    protected QueryExecutionEngine getEngine()
    {
        if ( this.engine == null )
        {
            this.engine = getServer().getDb().getDependencyResolver().resolveDependency( QueryExecutionEngine.class );
        }
        return this.engine;
    }

    protected long now()
    {
        return System.currentTimeMillis();
    }

    static QuerySession shellSession( Session session )
    {
        return new ShellQuerySession( session );
    }

    private static class ShellQuerySession extends QuerySession
    {
        private final Session session;

        ShellQuerySession( Session session )
        {
            this.session = session;
        }

        @Override
        public String toString()
        {
            return String.format("shell-session(%s)", session.getId());
        }
    }
}
