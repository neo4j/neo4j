/*
 * Copyright (c) 2002-2017 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.kernel.enterprise.builtinprocs;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.neo4j.function.UncaughtCheckedException;
import org.neo4j.graphdb.DependencyResolver;
import org.neo4j.graphdb.security.AuthorizationViolationException;
import org.neo4j.helpers.collection.Pair;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.api.KernelTransactionHandle;
import org.neo4j.kernel.api.Statement;
import org.neo4j.kernel.api.bolt.BoltConnectionTracker;
import org.neo4j.kernel.api.bolt.ManagedBoltStateMachine;
import org.neo4j.kernel.api.exceptions.InvalidArgumentsException;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.kernel.api.proc.ProcedureSignature;
import org.neo4j.kernel.api.proc.UserFunctionSignature;
import org.neo4j.kernel.api.query.ExecutingQuery;
import org.neo4j.kernel.api.security.SecurityContext;
import org.neo4j.kernel.impl.api.KernelTransactions;
import org.neo4j.kernel.impl.core.ThreadToStatementContextBridge;
import org.neo4j.kernel.impl.proc.Procedures;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import static java.lang.String.format;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toSet;
import static org.neo4j.function.ThrowingFunction.catchThrown;
import static org.neo4j.function.ThrowingFunction.throwIfPresent;
import static org.neo4j.graphdb.security.AuthorizationViolationException.PERMISSION_DENIED;
import static org.neo4j.kernel.enterprise.builtinprocs.QueryId.fromExternalString;
import static org.neo4j.kernel.enterprise.builtinprocs.QueryId.ofInternalId;
import static org.neo4j.procedure.Mode.DBMS;

@SuppressWarnings( "unused" )
public class EnterpriseBuiltInDbmsProcedures
{
    private static final int HARD_CHAR_LIMIT = 2048;

    @Context
    public DependencyResolver resolver;

    @Context
    public GraphDatabaseAPI graph;

    @Context
    public SecurityContext securityContext;

    @Description( "Attaches a map of data to the transaction. The data will be printed when listing queries, and " +
                  "inserted into the query log." )
    @Procedure( name = "dbms.setTXMetaData", mode = DBMS )
    public void setTXMetaData( @Name( value = "data" ) Map<String,Object> data )
    {
        int totalCharSize = data.entrySet().stream()
                .mapToInt( e -> e.getKey().length() + e.getValue().toString().length() )
                .sum();

        if ( totalCharSize >= HARD_CHAR_LIMIT )
        {
            throw new IllegalArgumentException(
                    format( "Invalid transaction meta-data, expected the total number of chars for " +
                            "keys and values to be less than %d, got %d", HARD_CHAR_LIMIT, totalCharSize ) );
        }

        try ( Statement statement = getCurrentTx().acquireStatement() )
        {
            statement.queryRegistration().setMetaData( data );
        }
    }

    private KernelTransaction getCurrentTx()
    {
        return graph.getDependencyResolver().resolveDependency( ThreadToStatementContextBridge.class )
                .getKernelTransactionBoundToThisThread( true );
    }

    /*
    This surface is hidden in 3.1, to possibly be completely removed or reworked later
    ==================================================================================
     */
    //@Procedure( name = "dbms.listTransactions", mode = DBMS )
    public Stream<TransactionResult> listTransactions()
            throws InvalidArgumentsException, IOException
    {
        assertAdmin();

        return countTransactionByUsername(
            getActiveTransactions( graph.getDependencyResolver() )
                .stream()
                .filter( tx -> !tx.terminationReason().isPresent() )
                .map( tx -> tx.securityContext().subject().username() )
        );
    }

    //@Procedure( name = "dbms.terminateTransactionsForUser", mode = DBMS )
    public Stream<TransactionTerminationResult> terminateTransactionsForUser( @Name( "username" ) String username )
            throws InvalidArgumentsException, IOException
    {
        assertAdminOrSelf( username );

        return terminateTransactionsForValidUser( graph.getDependencyResolver(), username, getCurrentTx() );
    }

    //@Procedure( name = "dbms.listConnections", mode = DBMS )
    public Stream<ConnectionResult> listConnections()
    {
        assertAdmin();

        BoltConnectionTracker boltConnectionTracker = getBoltConnectionTracker( graph.getDependencyResolver() );
        return countConnectionsByUsername(
            boltConnectionTracker
                .getActiveConnections()
                .stream()
                .filter( session -> !session.willTerminate() )
                .map( ManagedBoltStateMachine::owner )
        );
    }

    //@Procedure( name = "dbms.terminateConnectionsForUser", mode = DBMS )
    public Stream<ConnectionResult> terminateConnectionsForUser( @Name( "username" ) String username )
            throws InvalidArgumentsException
    {
        assertAdminOrSelf( username );

        return terminateConnectionsForValidUser( graph.getDependencyResolver(), username );
    }

    /*
    ==================================================================================
     */

    @Description( "List all user functions in the DBMS." )
    @Procedure( name = "dbms.functions", mode = DBMS )
    public Stream<FunctionResult> listFunctions()
    {
        return graph.getDependencyResolver().resolveDependency( Procedures.class ).getAllFunctions().stream()
                .sorted( Comparator.comparing( a -> a.name().toString() ) )
                .map( FunctionResult::new );
    }

    public static class FunctionResult
    {
        public final String name;
        public final String signature;
        public final String description;
        public final List<String> roles;

        private FunctionResult( UserFunctionSignature signature )
        {
            this.name = signature.name().toString();
            this.signature = signature.toString();
            this.description = signature.description().orElse( "" );
            roles = Stream.of( "admin", "reader", "editor", "publisher", "architect" ).collect( toList() );
            roles.addAll( Arrays.asList( signature.allowed() ) );
        }
    }

    @Description( "List all procedures in the DBMS." )
    @Procedure( name = "dbms.procedures", mode = DBMS )
    public Stream<ProcedureResult> listProcedures()
    {
        Procedures procedures = graph.getDependencyResolver().resolveDependency( Procedures.class );
        return procedures.getAllProcedures().stream()
                .sorted( Comparator.comparing( a -> a.name().toString() ) )
                .map( ProcedureResult::new );
    }

    @SuppressWarnings( "WeakerAccess" )
    public static class ProcedureResult
    {
        public final String name;
        public final String signature;
        public final String description;
        public final List<String> roles;

        public ProcedureResult( ProcedureSignature signature )
        {
            this.name = signature.name().toString();
            this.signature = signature.toString();
            this.description = signature.description().orElse( "" );
            roles = new ArrayList<>();
            switch ( signature.mode() )
            {
            case DBMS:
                roles.add( "admin" );
                break;
            case DEFAULT:
                roles.add( "reader" );
            case READ:
                roles.add( "reader" );
            case WRITE:
                roles.add( "editor" );
                roles.add( "publisher" );
            case SCHEMA:
                roles.add( "architect" );
            default:
                roles.add( "admin" );
                roles.addAll( Arrays.asList( signature.allowed() ) );
            }
        }
    }

    /*
    ==================================================================================
     */

    @Description( "List all queries currently executing at this instance that are visible to the user." )
    @Procedure( name = "dbms.listQueries", mode = DBMS )
    public Stream<QueryStatusResult> listQueries() throws InvalidArgumentsException, IOException
    {
        try
        {
            return getKernelTransactions().activeTransactions().stream()
                .flatMap( KernelTransactionHandle::executingQueries )
                .filter( query -> isAdminOrSelf( query.username() ) )
                .map( catchThrown( InvalidArgumentsException.class, QueryStatusResult::new ) );
        }
        catch ( UncaughtCheckedException uncaught )
        {
            throwIfPresent( uncaught.getCauseIfOfType( InvalidArgumentsException.class ) );
            throw uncaught;
        }
    }

    @Description( "List the active lock requests granted for the transaction executing the query with the given query id." )
    @Procedure( name = "dbms.listActiveLocks", mode = DBMS )
    public Stream<ActiveLocksQueryResult> listActiveLocks( @Name( "queryId" ) String queryId )
            throws InvalidArgumentsException
    {
        try
        {
            long id = fromExternalString( queryId ).kernelQueryId();
            return getActiveTransactions( tx -> executingQueriesWithId( id, tx ) )
                    .flatMap( this::getActiveLocksForQuery );
        }
        catch ( UncaughtCheckedException uncaught )
        {
            throwIfPresent( uncaught.getCauseIfOfType( InvalidArgumentsException.class ) );
            throw uncaught;
        }
    }

    @Description( "Kill all transactions executing the query with the given query id." )
    @Procedure( name = "dbms.killQuery", mode = DBMS )
    public Stream<QueryTerminationResult> killQuery( @Name( "id" ) String idText )
            throws InvalidArgumentsException, IOException
    {
        try
        {
            long queryId = fromExternalString( idText ).kernelQueryId();

            return getActiveTransactions( tx -> executingQueriesWithId( queryId, tx ) )
                .map( catchThrown( InvalidArgumentsException.class, this::killQueryTransaction ) );
         }
        catch ( UncaughtCheckedException uncaught )
        {
            throwIfPresent( uncaught.getCauseIfOfType( InvalidArgumentsException.class ) );
            throw uncaught;
        }
    }

    @Description( "Kill all transactions executing a query with any of the given query ids." )
    @Procedure( name = "dbms.killQueries", mode = DBMS )
    public Stream<QueryTerminationResult> killQueries( @Name( "ids" ) List<String> idTexts )
            throws InvalidArgumentsException, IOException
    {
        try
        {
            Set<Long> queryIds = idTexts
                .stream()
                .map( catchThrown( InvalidArgumentsException.class, QueryId::fromExternalString ) )
                .map( catchThrown( InvalidArgumentsException.class, QueryId::kernelQueryId ) )
                .collect( toSet() );

            return getActiveTransactions( tx -> executingQueriesWithIds( queryIds, tx ) )
                .map( catchThrown( InvalidArgumentsException.class, this::killQueryTransaction ) );
        }
        catch ( UncaughtCheckedException uncaught )
        {
            throwIfPresent( uncaught.getCauseIfOfType( InvalidArgumentsException.class ) );
            throw uncaught;
        }
    }

    private <T> Stream<Pair<KernelTransactionHandle, T>> getActiveTransactions(
            Function<KernelTransactionHandle,Stream<T>> selector
    )
    {
        return getActiveTransactions( graph.getDependencyResolver() )
            .stream()
            .flatMap( tx -> selector.apply( tx ).map( data -> Pair.of( tx, data ) ) );
    }

    private Stream<ExecutingQuery> executingQueriesWithIds( Set<Long> ids, KernelTransactionHandle txHandle )
    {
        return txHandle.executingQueries().filter( q -> ids.contains( q.internalQueryId() ) );
    }

    private Stream<ExecutingQuery> executingQueriesWithId( long id, KernelTransactionHandle txHandle )
    {
        return txHandle.executingQueries().filter( q -> q.internalQueryId() == id );
    }

    private QueryTerminationResult killQueryTransaction( Pair<KernelTransactionHandle, ExecutingQuery> pair )
            throws InvalidArgumentsException
    {
        ExecutingQuery query = pair.other();
        if ( isAdminOrSelf( query.username() ) )
        {
            pair.first().markForTermination( Status.Transaction.Terminated );
            return new QueryTerminationResult( ofInternalId( query.internalQueryId() ), query.username() );
        }
        else
        {
            throw new AuthorizationViolationException( PERMISSION_DENIED );
        }
    }

    private Stream<ActiveLocksQueryResult> getActiveLocksForQuery( Pair<KernelTransactionHandle, ExecutingQuery> pair )
    {
        ExecutingQuery query = pair.other();
        if ( isAdminOrSelf( query.username() ) )
        {
            return pair.first().activeLocks().map( ActiveLocksQueryResult::new );
        }
        else
        {
            throw new AuthorizationViolationException( PERMISSION_DENIED );
        }
    }

    private KernelTransactions getKernelTransactions()
    {
        return resolver.resolveDependency( KernelTransactions.class );
    }

    // ----------------- helpers ---------------------

    public static Stream<TransactionTerminationResult> terminateTransactionsForValidUser(
            DependencyResolver dependencyResolver, String username, KernelTransaction currentTx )
    {
        long terminatedCount = getActiveTransactions( dependencyResolver )
            .stream()
            .filter( tx -> tx.securityContext().subject().hasUsername( username ) &&
                            !tx.isUnderlyingTransaction( currentTx ) )
            .map( tx -> tx.markForTermination( Status.Transaction.Terminated ) )
            .filter( marked -> marked )
            .count();
        return Stream.of( new TransactionTerminationResult( username, terminatedCount ) );
    }

    public static Stream<ConnectionResult> terminateConnectionsForValidUser(
            DependencyResolver dependencyResolver, String username )
    {
        Long killCount = getBoltConnectionTracker( dependencyResolver )
            .getActiveConnections( username )
            .stream().map( conn ->
                {
                    conn.terminate();
                    return true;
                } )
            .count();
        return Stream.of( new ConnectionResult( username, killCount ) );
    }

    public static Set<KernelTransactionHandle> getActiveTransactions( DependencyResolver dependencyResolver )
    {
        return dependencyResolver.resolveDependency( KernelTransactions.class ).activeTransactions();
    }

    public static BoltConnectionTracker getBoltConnectionTracker( DependencyResolver dependencyResolver )
    {
        return dependencyResolver.resolveDependency( BoltConnectionTracker.class );
    }

    public static Stream<TransactionResult> countTransactionByUsername( Stream<String> usernames )
    {
        return usernames
            .collect( Collectors.groupingBy( Function.identity(), Collectors.counting() ) )
            .entrySet()
            .stream()
            .map( entry -> new TransactionResult( entry.getKey(), entry.getValue() )
        );
    }

    public static Stream<ConnectionResult> countConnectionsByUsername( Stream<String> usernames )
    {
        return usernames
            .collect( Collectors.groupingBy( Function.identity(), Collectors.counting() ) )
            .entrySet()
            .stream()
            .map( entry -> new ConnectionResult( entry.getKey(), entry.getValue() ) );
    }

    private boolean isAdmin()
    {
        return securityContext.isAdmin();
    }

    private void assertAdmin()
    {
        if ( !isAdmin() )
        {
            throw new AuthorizationViolationException( PERMISSION_DENIED );
        }
    }

    private boolean isAdminOrSelf( String username )
    {
        return isAdmin() || securityContext.subject().hasUsername( username );
    }

    private void assertAdminOrSelf( String username )
            throws InvalidArgumentsException
    {
        if ( !isAdminOrSelf( username ) )
        {
            throw new AuthorizationViolationException( PERMISSION_DENIED );
        }
    }

    public static class QueryTerminationResult
    {
        public final String queryId;
        public final String username;

        public QueryTerminationResult( QueryId queryId, String username )
        {
            this.queryId = queryId.toString();
            this.username = username;
        }
    }

    public static class TransactionResult
    {
        public final String username;
        public final Long activeTransactions;

        TransactionResult( String username, Long activeTransactions )
        {
            this.username = username;
            this.activeTransactions = activeTransactions;
        }
    }

    public static class TransactionTerminationResult
    {
        public final String username;
        public final Long transactionsTerminated;

        TransactionTerminationResult( String username, Long transactionsTerminated )
        {
            this.username = username;
            this.transactionsTerminated = transactionsTerminated;
        }
    }

    public static class ConnectionResult
    {
        public final String username;
        public final Long connectionCount;

        ConnectionResult( String username, Long connectionCount )
        {
            this.username = username;
            this.connectionCount = connectionCount;
        }
    }
}
