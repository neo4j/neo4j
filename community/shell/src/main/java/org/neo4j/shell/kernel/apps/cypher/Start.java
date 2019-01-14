/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
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

import org.neo4j.graphdb.DependencyResolver;
import org.neo4j.graphdb.Result;
import org.neo4j.helpers.Service;
import org.neo4j.internal.kernel.api.security.LoginContext;
import org.neo4j.kernel.GraphDatabaseQueryService;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.impl.coreapi.InternalTransaction;
import org.neo4j.kernel.impl.coreapi.PropertyContainerLocker;
import org.neo4j.kernel.impl.query.Neo4jTransactionalContextFactory;
import org.neo4j.kernel.impl.query.QueryExecutionEngine;
import org.neo4j.kernel.impl.query.QueryExecutionKernelException;
import org.neo4j.kernel.impl.query.TransactionalContext;
import org.neo4j.kernel.impl.query.TransactionalContextFactory;
import org.neo4j.kernel.impl.query.clientconnection.ShellConnectionInfo;
import org.neo4j.kernel.impl.util.ValueUtils;
import org.neo4j.shell.App;
import org.neo4j.shell.AppCommandParser;
import org.neo4j.shell.Continuation;
import org.neo4j.shell.Output;
import org.neo4j.shell.OutputAsWriter;
import org.neo4j.shell.Session;
import org.neo4j.shell.ShellException;
import org.neo4j.shell.kernel.apps.NodeOrRelationship;
import org.neo4j.shell.kernel.apps.TransactionProvidingApp;

@Service.Implementation( App.class )
public class Start extends TransactionProvidingApp
{
    private QueryExecutionEngine engine;

    @Override
    public String getDescription()
    {
        String className = this.getClass().getSimpleName().toUpperCase();
        return MessageFormat.format( "Executes a Cypher query. Usage: {0} <rest of query>;\nExample: MATCH " +
                                     "(me)-[:KNOWS]->(you) RETURN you.name;\nwhere '{'self'}' will be replaced with " +
                                     "the current location in the graph.Please, note that the query must end with a " +
                                     "semicolon. Other parameters are\ntaken from shell variables, " +
                                     "see ''help export''.", className );
    }

    @Override
    public Continuation execute( AppCommandParser parser, Session session, Output out ) throws Exception
    {

        String query = parser.getLine().trim();

        if ( isComplete( query ) )
        {
            if ( getEngine().isPeriodicCommit( query ) )
            {
                return exec( parser, session, out );
            }
            else
            {
                return super.execute( parser, session, out );
            }
        }
        else
        {
            return Continuation.INPUT_INCOMPLETE;
        }

    }

    @Override
    protected Continuation exec( AppCommandParser parser, Session session, Output out )
            throws RemoteException
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

    private Result getResult( String query, Session session )
            throws QueryExecutionKernelException
    {
        Map<String,Object> parameters = getParameters( session );
        TransactionalContext tc = createTransactionContext( query, parameters, session );
        return getEngine().executeQuery( query, parameters, tc );
    }

    private String trimQuery( String query )
    {
        return query.substring( 0, query.lastIndexOf( ';' ) );
    }

    protected void handleResult( Output out, Result result, long startTime )
            throws RemoteException
    {
        printResult( out, result, startTime );
        result.close();
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

    private void handleException( Output out, QueryExecutionKernelException exception, long startTime )
            throws RemoteException
    {
        out.println( (now() - startTime) + " ms" );
        out.println();
        out.println( "WARNING: " + exception.getMessage() );
    }

    private Map<String,Object> getParameters( Session session )
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

    private boolean isComplete( String query )
    {
        return query.endsWith( ";" );
    }

    protected QueryExecutionEngine getEngine()
    {
        if ( this.engine == null )
        {
            this.engine = getDependencyResolver().resolveDependency( QueryExecutionEngine.class );
        }
        return this.engine;
    }

    private DependencyResolver getDependencyResolver()
    {
        return getServer().getDb().getDependencyResolver();
    }

    protected long now()
    {
        return System.currentTimeMillis();
    }

    private TransactionalContext createTransactionContext( String queryText, Map<String,Object> queryParameters,
            Session session )
    {
        DependencyResolver dependencyResolver = getDependencyResolver();
        GraphDatabaseQueryService graph = dependencyResolver.resolveDependency( GraphDatabaseQueryService.class );
        TransactionalContextFactory contextFactory =
            Neo4jTransactionalContextFactory.create( graph, new PropertyContainerLocker() );
        InternalTransaction transaction =
            graph.beginTransaction( KernelTransaction.Type.implicit, LoginContext.AUTH_DISABLED );
        return contextFactory.newContext(
                new ShellConnectionInfo( session.getId() ),
                transaction,
                queryText,
                ValueUtils.asParameterMapValue( queryParameters )
        );
    }
}
