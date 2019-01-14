/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * GNU AFFERO GENERAL PUBLIC LICENSE Version 3
 * (http://www.fsf.org/licensing/licenses/agpl-3.0.html) with the
 * Commons Clause, as found in the associated LICENSE.txt file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * Neo4j object code can be licensed independently from the source
 * under separate terms from the AGPL. Inquiries can be directed to:
 * licensing@neo4j.com
 *
 * More information is also available at:
 * https://neo4j.com/licensing/
 */
package org.neo4j.kernel.enterprise.builtinprocs;

import java.time.ZoneId;
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
import org.neo4j.internal.kernel.api.procs.ProcedureSignature;
import org.neo4j.internal.kernel.api.procs.UserFunctionSignature;
import org.neo4j.internal.kernel.api.security.SecurityContext;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.api.KernelTransactionHandle;
import org.neo4j.kernel.api.Statement;
import org.neo4j.kernel.api.bolt.BoltConnectionTracker;
import org.neo4j.kernel.api.bolt.ManagedBoltStateMachine;
import org.neo4j.kernel.api.exceptions.InvalidArgumentsException;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.kernel.api.query.ExecutingQuery;
import org.neo4j.kernel.api.query.QuerySnapshot;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.api.KernelTransactions;
import org.neo4j.kernel.impl.core.EmbeddedProxySPI;
import org.neo4j.kernel.impl.core.ThreadToStatementContextBridge;
import org.neo4j.kernel.impl.proc.Procedures;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import static java.lang.String.format;
import static java.util.function.Function.identity;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toMap;
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
    //@Procedure( name = "dbms.terminateTransactionsForUser", mode = DBMS )
    public Stream<TransactionTerminationResult> terminateTransactionsForUser( @Name( "username" ) String username )
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
        public final String mode;

        public ProcedureResult( ProcedureSignature signature )
        {
            this.name = signature.name().toString();
            this.signature = signature.toString();
            this.description = signature.description().orElse( "" );
            this.mode = signature.mode().toString();
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
                    name.equals( "dbms.setConfigValue" ) ||
                    name.equals( "dbms.clearQueryCaches" );
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
        config.updateDynamicSetting( setting, value, "dbms.setConfigValue" ); // throws if something goes wrong
    }

    /*
    ==================================================================================
     */

    @Description( "List all queries currently executing at this instance that are visible to the user." )
    @Procedure( name = "dbms.listQueries", mode = DBMS )
    public Stream<QueryStatusResult> listQueries() throws InvalidArgumentsException
    {
        securityContext.assertCredentialsNotExpired();

        EmbeddedProxySPI nodeManager = resolver.resolveDependency( EmbeddedProxySPI.class );
        ZoneId zoneId = getConfiguredTimeZone();
        try
        {
            return getKernelTransactions().activeTransactions().stream()
                .flatMap( KernelTransactionHandle::executingQueries )
                    .filter( query -> isAdminOrSelf( query.username() ) )
                    .map( catchThrown( InvalidArgumentsException.class,
                            query -> new QueryStatusResult( query, nodeManager, zoneId ) ) );
        }
        catch ( UncaughtCheckedException uncaught )
        {
            throwIfPresent( uncaught.getCauseIfOfType( InvalidArgumentsException.class ) );
            throw uncaught;
        }
    }

    @Description( "List all transactions currently executing at this instance that are visible to the user." )
    @Procedure( name = "dbms.listTransactions", mode = DBMS )
    public Stream<TransactionStatusResult> listTransactions() throws InvalidArgumentsException
    {
        securityContext.assertCredentialsNotExpired();
        try
        {
            Set<KernelTransactionHandle> handles = getKernelTransactions().activeTransactions().stream()
                    .filter( transaction -> isAdminOrSelf( transaction.subject().username() ) )
                    .collect( toSet() );

            Map<KernelTransactionHandle,List<QuerySnapshot>> handleQuerySnapshotsMap = handles.stream()
                    .collect( toMap( identity(), getTransactionQueries() ) );

            TransactionDependenciesResolver transactionBlockerResolvers =
                    new TransactionDependenciesResolver( handleQuerySnapshotsMap );

            ZoneId zoneId = getConfiguredTimeZone();

            return handles.stream()
                    .map( catchThrown( InvalidArgumentsException.class,
                            tx -> new TransactionStatusResult( tx, transactionBlockerResolvers,
                                    handleQuerySnapshotsMap, zoneId ) ) );
        }
        catch ( UncaughtCheckedException uncaught )
        {
            throwIfPresent( uncaught.getCauseIfOfType( InvalidArgumentsException.class ) );
            throw uncaught;
        }
    }

    private Function<KernelTransactionHandle,List<QuerySnapshot>> getTransactionQueries()
    {
        return transactionHandle -> transactionHandle.executingQueries()
                                              .map( ExecutingQuery::snapshot )
                                              .collect( toList() );
    }

    @Description( "List the active lock requests granted for the transaction executing the query with the given query id." )
    @Procedure( name = "dbms.listActiveLocks", mode = DBMS )
    public Stream<ActiveLocksResult> listActiveLocks( @Name( "queryId" ) String queryId )
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
    public Stream<QueryTerminationResult> killQuery( @Name( "id" ) String idText ) throws InvalidArgumentsException
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
    public Stream<QueryTerminationResult> killQueries( @Name( "ids" ) List<String> idTexts ) throws InvalidArgumentsException
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
                    if ( terminatedQuerys.stream().noneMatch( query -> query.queryId.equals( id ) ) )
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

    private Stream<ActiveLocksResult> getActiveLocksForQuery( Pair<KernelTransactionHandle, ExecutingQuery> pair )
    {
        ExecutingQuery query = pair.other();
        if ( isAdminOrSelf( query.username() ) )
        {
            return pair.first().activeLocks().map( ActiveLocksResult::new );
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
            .filter( tx -> tx.subject().hasUsername( username ) &&
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
            .collect( Collectors.groupingBy( identity(), Collectors.counting() ) )
            .entrySet()
            .stream()
            .map( entry -> new TransactionResult( entry.getKey(), entry.getValue() )
        );
    }

    public static Stream<ConnectionResult> countConnectionsByUsername( Stream<String> usernames )
    {
        return usernames
            .collect( Collectors.groupingBy( identity(), Collectors.counting() ) )
            .entrySet()
            .stream()
            .map( entry -> new ConnectionResult( entry.getKey(), entry.getValue() ) );
    }

    private ZoneId getConfiguredTimeZone()
    {
        Config config = resolver.resolveDependency( Config.class );
        return config.get( GraphDatabaseSettings.db_timezone ).getZoneId();
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
