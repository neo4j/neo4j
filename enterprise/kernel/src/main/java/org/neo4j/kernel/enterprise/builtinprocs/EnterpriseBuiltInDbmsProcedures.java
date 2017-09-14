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
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
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
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.api.KernelTransactions;
import org.neo4j.kernel.impl.core.NodeManager;
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
        securityContext.assertCredentialsNotExpired();
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

    @Description( "Provides attached transaction metadata." )
    @Procedure( name = "dbms.getTXMetaData", mode = DBMS )
    public Stream<MetadataResult> getTXMetaData()
    {
        securityContext.assertCredentialsNotExpired();
        try ( Statement statement = getCurrentTx().acquireStatement() )
        {
            return Stream.of( statement.queryRegistration().getMetaData() ).map( MetadataResult::new );
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
        securityContext.assertCredentialsNotExpired();
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
        securityContext.assertCredentialsNotExpired();
        Procedures procedures = graph.getDependencyResolver().resolveDependency( Procedures.class );
        return procedures.getAllProcedures().stream()
                .sorted( Comparator.comparing( a -> a.name().toString() ) )
                .map( ProcedureResult::new );
    }

    @SuppressWarnings( "WeakerAccess" )
    public static class ProcedureResult
    {
        private static final List<String> ADMIN_PROCEDURES =
                Arrays.asList( "createUser", "deleteUser", "listUsers", "clearAuthCache", "changeUserPassword",
                        "addRoleToUser", "removeRoleFromUser", "suspendUser", "activateUser", "listRoles",
                        "listRolesForUser", "listUsersForRole", "createRole", "deleteRole" );

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
                // TODO: not enough granularity for dbms and user management, needs fix
                if ( isAdminProcedure( signature.name().name() ) )
                {
                    roles.add( "admin" );
                }
                else
                {
                    roles.add( "reader" );
                    roles.add( "editor" );
                    roles.add( "publisher" );
                    roles.add( "architect" );
                    roles.add( "admin" );
                    roles.addAll( Arrays.asList( signature.allowed() ) );
                }
                break;
            case DEFAULT:
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

        private boolean isAdminProcedure( String procedureName )
        {
            return name.startsWith( "dbms.security." ) && ADMIN_PROCEDURES.contains( procedureName ) ||
                   name.equals( "dbms.listConfig" ) ||
                   name.equals( "dbms.setConfigValue" );
        }
    }

    @Description( "Updates a given setting value. Passing an empty value will result in removing the configured value " +
            "and falling back to the default value. Changes will not persist and will be lost if the server is restarted." )
    @Procedure( name = "dbms.setConfigValue", mode = DBMS )
    public void setConfigValue( @Name( "setting" ) String setting, @Name( "value" ) String value )
    {
        securityContext.assertCredentialsNotExpired();
        assertAdmin();

        Config config = resolver.resolveDependency( Config.class );
        config.updateDynamicSetting( setting, value ); // throws if something goes wrong
    }

    /*
    ==================================================================================
     */

    @Description( "List all queries currently executing at this instance that are visible to the user." )
    @Procedure( name = "dbms.listQueries", mode = DBMS )
    public Stream<QueryStatusResult> listQueries() throws InvalidArgumentsException, IOException
    {
        securityContext.assertCredentialsNotExpired();
        NodeManager nodeManager = resolver.resolveDependency( NodeManager.class );
        try
        {
            return getKernelTransactions().activeTransactions().stream()
                .flatMap( KernelTransactionHandle::executingQueries )
                    .filter( query -> isAdminOrSelf( query.username() ) )
                    .map( catchThrown( InvalidArgumentsException.class,
                            query -> new QueryStatusResult( query, nodeManager ) ) );
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
        securityContext.assertCredentialsNotExpired();
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
    public Stream<QueryTerminationResult> killQuery( @Name( "id" ) String idText ) throws InvalidArgumentsException, IOException
    {
        securityContext.assertCredentialsNotExpired();
        try
        {
            long queryId = fromExternalString( idText ).kernelQueryId();

            Set<Pair<KernelTransactionHandle,ExecutingQuery>> querys = getActiveTransactions( tx -> executingQueriesWithId( queryId, tx ) ).collect( toSet() );
            boolean killQueryVerbose = resolver.resolveDependency( Config.class ).get( GraphDatabaseSettings.kill_query_verbose );
            if ( killQueryVerbose && querys.isEmpty() )
            {
                return Stream.<QueryTerminationResult>builder().add( new QueryFailedTerminationResult( fromExternalString( idText ) ) ).build();
            }
            return querys.stream().map( catchThrown( InvalidArgumentsException.class, this::killQueryTransaction ) );
        }
        catch ( UncaughtCheckedException uncaught )
        {
            throwIfPresent( uncaught.getCauseIfOfType( InvalidArgumentsException.class ) );
            throw uncaught;
        }
    }

    @Description( "Kill all transactions executing a query with any of the given query ids." )
    @Procedure( name = "dbms.killQueries", mode = DBMS )
    public Stream<QueryTerminationResult> killQueries( @Name( "ids" ) List<String> idTexts ) throws InvalidArgumentsException, IOException
    {
        securityContext.assertCredentialsNotExpired();
        try
        {

            Set<Long> queryIds = idTexts.stream().map( catchThrown( InvalidArgumentsException.class, QueryId::fromExternalString ) ).map(
                    catchThrown( InvalidArgumentsException.class, QueryId::kernelQueryId ) ).collect( toSet() );

            Set<QueryTerminationResult> terminatedQuerys = getActiveTransactions( tx -> executingQueriesWithIds( queryIds, tx ) ).map(
                    catchThrown( InvalidArgumentsException.class, this::killQueryTransaction ) ).collect( toSet() );
            boolean killQueryVerbose = resolver.resolveDependency( Config.class ).get( GraphDatabaseSettings.kill_query_verbose );
            if ( killQueryVerbose && terminatedQuerys.size() != idTexts.size() )
            {
                for ( String id : idTexts )
                {
                    if ( !terminatedQuerys.stream().anyMatch( query -> query.queryId.equals( id ) ) )
                    {
                        terminatedQuerys.add( new QueryFailedTerminationResult( fromExternalString( id ) ) );
                    }
                }
            }
            return terminatedQuerys.stream();
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
        public String message = "Query found";

        public QueryTerminationResult( QueryId queryId, String username )
        {
            this.queryId = queryId.toString();
            this.username = username;
        }
    }

    public static class QueryFailedTerminationResult extends QueryTerminationResult
    {
        public QueryFailedTerminationResult( QueryId queryId )
        {
            super( queryId, "n/a" );
            super.message = "No Query found with this id";
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

    public static class MetadataResult
    {
        public final Map<String,Object> metadata;

        MetadataResult( Map<String,Object> metadata )
        {
            this.metadata = metadata;
        }
    }
}
