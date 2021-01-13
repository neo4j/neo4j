/*
 * Copyright (c) 2002-2020 "Neo4j,"
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
package org.neo4j.procedure.builtin;

import java.security.NoSuchAlgorithmException;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.neo4j.collection.Dependencies;
import org.neo4j.common.DependencyResolver;
import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseInternalSettings;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.dbms.database.DatabaseContext;
import org.neo4j.dbms.database.DatabaseManager;
import org.neo4j.dbms.database.SystemGraphComponent;
import org.neo4j.dbms.database.SystemGraphComponents;
import org.neo4j.fabric.executor.FabricStatementLifecycles;
import org.neo4j.fabric.transaction.FabricTransaction;
import org.neo4j.fabric.transaction.TransactionManager;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.security.AuthorizationViolationException;
import org.neo4j.internal.kernel.api.exceptions.ProcedureException;
import org.neo4j.internal.kernel.api.procs.ProcedureCallContext;
import org.neo4j.internal.kernel.api.procs.ProcedureSignature;
import org.neo4j.internal.kernel.api.procs.UserFunctionSignature;
import org.neo4j.internal.kernel.api.security.AdminActionOnResource;
import org.neo4j.internal.kernel.api.security.AdminActionOnResource.DatabaseScope;
import org.neo4j.internal.kernel.api.security.SecurityContext;
import org.neo4j.internal.kernel.api.security.UserSegment;
import org.neo4j.kernel.api.KernelTransactionHandle;
import org.neo4j.kernel.api.exceptions.InvalidArgumentsException;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.kernel.api.net.NetworkConnectionTracker;
import org.neo4j.kernel.api.net.TrackedNetworkConnection;
import org.neo4j.kernel.api.procedure.GlobalProcedures;
import org.neo4j.kernel.api.procedure.SystemProcedure;
import org.neo4j.kernel.api.query.ExecutingQuery;
import org.neo4j.kernel.api.query.QuerySnapshot;
import org.neo4j.kernel.database.DatabaseIdRepository;
import org.neo4j.kernel.database.NamedDatabaseId;
import org.neo4j.kernel.impl.api.KernelTransactions;
import org.neo4j.kernel.impl.coreapi.InternalTransaction;
import org.neo4j.kernel.impl.query.FunctionInformation;
import org.neo4j.kernel.impl.query.QueryExecutionEngine;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.logging.Log;
import org.neo4j.procedure.Admin;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Internal;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;
import org.neo4j.storageengine.api.StoreIdProvider;

import static java.lang.String.format;
import static java.util.Collections.singletonList;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;
import static org.neo4j.configuration.GraphDatabaseSettings.SYSTEM_DATABASE_NAME;
import static org.neo4j.dbms.database.SystemGraphComponent.Status.REQUIRES_UPGRADE;
import static org.neo4j.dbms.database.SystemGraphComponent.Status.UNINITIALIZED;
import static org.neo4j.graphdb.security.AuthorizationViolationException.PERMISSION_DENIED;
import static org.neo4j.internal.kernel.api.security.AdminActionOnResource.DatabaseScope.ALL;
import static org.neo4j.internal.kernel.api.security.PrivilegeAction.SHOW_TRANSACTION;
import static org.neo4j.internal.kernel.api.security.PrivilegeAction.TERMINATE_TRANSACTION;
import static org.neo4j.kernel.api.exceptions.Status.Procedure.ProcedureCallFailed;
import static org.neo4j.procedure.Mode.DBMS;
import static org.neo4j.procedure.Mode.READ;
import static org.neo4j.procedure.Mode.WRITE;
import static org.neo4j.procedure.builtin.ProceduresTimeFormatHelper.formatTime;
import static org.neo4j.procedure.builtin.StoreIdDecodeUtils.decodeId;

@SuppressWarnings( "unused" )
public class BuiltInDbmsProcedures
{
    private static final int HARD_CHAR_LIMIT = 2048;

    @Context
    public Log log;

    @Context
    public DependencyResolver resolver;

    @Context
    public GraphDatabaseAPI graph;

    @Context
    public Transaction transaction;

    @Context
    public SecurityContext securityContext;

    @Context
    public ProcedureCallContext callContext;

    @Context
    public SystemGraphComponents systemGraphComponents;

    @SystemProcedure
    @Description( "Provides information regarding the DBMS." )
    @Procedure( name = "dbms.info", mode = DBMS )
    public Stream<SystemInfo> databaseInfo() throws NoSuchAlgorithmException
    {
        var systemGraph = getSystemDatabase();
        var storeIdProvider = getSystemDatabaseStoreIdProvider( systemGraph );
        var creationTime = formatTime( storeIdProvider.getStoreId().getCreationTime(), getConfiguredTimeZone() );
        return Stream.of( new SystemInfo( decodeId( storeIdProvider ), systemGraph.databaseName(), creationTime ) );
    }

    @Admin
    @SystemProcedure
    @Description( "List the currently active config of Neo4j." )
    @Procedure( name = "dbms.listConfig", mode = DBMS )
    public Stream<ConfigResult> listConfig( @Name( value = "searchString", defaultValue = "" ) String searchString )
    {
        String lowerCasedSearchString = searchString.toLowerCase();
        List<ConfigResult> results = new ArrayList<>();

        Config config = graph.getDependencyResolver().resolveDependency( Config.class );

        config.getValues().forEach( ( setting, value ) -> {
            if ( !setting.internal() && setting.name().toLowerCase().contains( lowerCasedSearchString ) )
            {
                results.add( new ConfigResult( setting, value ) );
            }
        } );
        return results.stream().sorted( Comparator.comparing( c -> c.name ) );
    }

    @Internal
    @SystemProcedure
    @Description( "Return config settings interesting to clients (e.g. Neo4j Browser)" )
    @Procedure( name = "dbms.clientConfig", mode = DBMS )
    public Stream<ConfigResult> listClientConfig()
    {
        List<ConfigResult> results = new ArrayList<>();
        Set<String> browserSettings = Stream.of( "browser.allow_outgoing_connections",
                                                 "browser.credential_timeout",
                                                 "browser.retain_connection_credentials",
                                                 "dbms.security.auth_enabled",
                                                 "browser.remote_content_hostname_whitelist",
                                                 "browser.post_connect_cmd",
                                                 "dbms.default_database" ).collect( Collectors.toCollection( HashSet::new ) );

        Config config = graph.getDependencyResolver().resolveDependency( Config.class );
        config.getValues().forEach( ( setting, value ) ->
        {
            if ( browserSettings.contains( setting.name().toLowerCase() ) )
            {
                results.add( new ConfigResult( setting, value ) );
            }
        } );
        return results.stream().sorted( Comparator.comparing( c -> c.name ) );
    }

    @Description( "Attaches a map of data to the transaction. The data will be printed when listing queries, and " +
            "inserted into the query log." )
    @Procedure( name = "tx.setMetaData", mode = DBMS )
    public void setTXMetaData( @Name( value = "data" ) Map<String,Object> data )
    {
        securityContext.assertCredentialsNotExpired();
        int totalCharSize = data.entrySet()
                .stream()
                .mapToInt( e -> e.getKey().length() + ((e.getValue() != null) ? e.getValue().toString().length() : 0) )
                .sum();

        if ( totalCharSize >= HARD_CHAR_LIMIT )
        {
            throw new IllegalArgumentException(
                    format( "Invalid transaction meta-data, expected the total number of chars for " +
                            "keys and values to be less than %d, got %d", HARD_CHAR_LIMIT, totalCharSize ) );
        }

        InternalTransaction internalTransaction = (InternalTransaction) this.transaction;

        graph.getDependencyResolver().resolveDependency( TransactionManager.class )
             .findTransactionContaining( internalTransaction )
             .ifPresentOrElse(
                     parent -> parent.setMetaData( data ),
                     () -> internalTransaction.setMetaData( data )
             );
    }

    @SystemProcedure
    @Description( "Provides attached transaction metadata." )
    @Procedure( name = "tx.getMetaData", mode = DBMS )
    public Stream<MetadataResult> getTXMetaData()
    {
        securityContext.assertCredentialsNotExpired();
        return Stream.of( ((InternalTransaction) transaction).kernelTransaction().getMetaData() ).map( MetadataResult::new );
    }

    @SystemProcedure
    @Description( "List all procedures in the DBMS." )
    @Procedure( name = "dbms.procedures", mode = DBMS )
    public Stream<ProcedureResult> listProcedures()
    {
        securityContext.assertCredentialsNotExpired();
        return graph.getDependencyResolver().resolveDependency( GlobalProcedures.class ).getAllProcedures().stream()
                .filter( proc -> !proc.internal() )
                .sorted( Comparator.comparing( a -> a.name().toString() ) )
                .map( ProcedureResult::new );
    }

    @SystemProcedure
    @Description( "List all functions in the DBMS." )
    @Procedure( name = "dbms.functions", mode = DBMS )
    public Stream<FunctionResult> listFunctions()
    {
        securityContext.assertCredentialsNotExpired();

        DependencyResolver resolver = graph.getDependencyResolver();
        QueryExecutionEngine queryExecutionEngine = resolver.resolveDependency( QueryExecutionEngine.class );
        List<FunctionInformation> providedLanguageFunctions = queryExecutionEngine.getProvidedLanguageFunctions();
        var globalProcedures = resolver.resolveDependency( GlobalProcedures.class );

        // gets you all functions provided by the query language
        Stream<FunctionResult> languageFunctions =
                providedLanguageFunctions.stream().map( FunctionResult::new );

        // gets you all non-aggregating functions that are registered in the db (incl. those from libs like apoc)
        Stream<FunctionResult> loadedFunctions = globalProcedures.getAllNonAggregatingFunctions()
                .map( f -> new FunctionResult( f, false ) );

        // gets you all aggregation functions that are registered in the db (incl. those from libs like apoc)
        Stream<FunctionResult> loadedAggregationFunctions = globalProcedures.getAllAggregatingFunctions()
                .map( f -> new FunctionResult( f, true ) );

        return Stream.concat( Stream.concat( languageFunctions, loadedFunctions ), loadedAggregationFunctions )
                .sorted( Comparator.comparing( a -> a.name ) );
    }

    @Admin
    @SystemProcedure
    @Description( "Clears all query caches." )
    @Procedure( name = "db.clearQueryCaches", mode = DBMS )
    public Stream<StringResult> clearAllQueryCaches()
    {
        QueryExecutionEngine queryExecutionEngine = graph.getDependencyResolver().resolveDependency( QueryExecutionEngine.class );
        long numberOfClearedQueries = queryExecutionEngine.clearQueryCaches() - 1; // this query itself does not count

        String result = numberOfClearedQueries == 0 ? "Query cache already empty."
                                                    : "Query caches successfully cleared of " + numberOfClearedQueries + " queries.";
        log.info( "Called db.clearQueryCaches(): " + result );
        return Stream.of( new StringResult( result ) );
    }

    @Admin
    @SystemProcedure
    @Description( "Report the current status of the system database sub-graph schema." )
    @Procedure( name = "dbms.upgradeStatus", mode = READ )
    public Stream<SystemGraphComponentStatusResult> upgradeStatus() throws ProcedureException
    {
        assertAllowedUpgradeProc();
        if ( !callContext.isSystemDatabase() )
        {
            throw new ProcedureException( ProcedureCallFailed,
                                          "This is an administration command and it should be executed against the system database: dbms.upgradeStatus" );
        }
        return Stream.of( new SystemGraphComponentStatusResult( systemGraphComponents.detect( transaction ) ) );
    }

    @Admin
    @SystemProcedure
    @Description( "Upgrade the system database schema if it is not the current schema." )
    @Procedure( name = "dbms.upgrade", mode = WRITE )
    public Stream<SystemGraphComponentUpgradeResult> upgrade() throws ProcedureException
    {
        assertAllowedUpgradeProc();
        if ( !callContext.isSystemDatabase() )
        {
            throw new ProcedureException( ProcedureCallFailed,
                                          "This is an administration command and it should be executed against the system database: dbms.upgrade" );
        }
        SystemGraphComponents versions = systemGraphComponents;
        SystemGraphComponent.Status status = versions.detect( transaction );

        // New components are not currently initialised in cluster deployment when new binaries are booted on top of an existing database.
        // This is a known shortcoming of the lifecycle and a state transfer from UNINITIALIZED to CURRENT must be supported
        // as a workaround until it is fixed.
        var upgradableStatuses = List.of( REQUIRES_UPGRADE, UNINITIALIZED );

        if ( upgradableStatuses.contains( status ) )
        {
            ArrayList<String> failed = new ArrayList<>();
            versions.forEach( component ->
                              {
                                  SystemGraphComponent.Status initialStatus = component.detect( transaction );
                                  if ( upgradableStatuses.contains( initialStatus ) )
                                  {
                                      try
                                      {
                                         component.upgradeToCurrent( graph );
                                      }
                                      catch ( Exception e )
                                      {
                                          failed.add( String.format( "[%s] %s", component.componentName(), e.getMessage() ) );
                                      }
                                  }
                              } );
            String upgradeResult = failed.isEmpty() ? "Success" : "Failed: " + String.join( ", ", failed );
            return Stream.of( new SystemGraphComponentUpgradeResult( versions.detect( transaction ).name(), upgradeResult ) );
        }
        else
        {
            return Stream.of( new SystemGraphComponentUpgradeResult( status.name(), status.resolution() ) );
        }
    }

    private void assertAllowedUpgradeProc()
    {
        Config config = graph.getDependencyResolver().resolveDependency( Config.class );
        if ( config.get( GraphDatabaseInternalSettings.restrict_upgrade ) )
        {
            if ( !securityContext.subject().hasUsername( config.get( GraphDatabaseInternalSettings.upgrade_username ) ) )
            {
                throw new AuthorizationViolationException(
                        String.format( "%s Execution of this procedure has been restricted by the system.", PERMISSION_DENIED ) );
            }
        }
    }

    @SystemProcedure
    @Description( "List all transactions currently executing at this instance that are visible to the user." )
    @Procedure( name = "dbms.listTransactions", mode = DBMS )
    public Stream<TransactionStatusResult> listTransactions() throws InvalidArgumentsException
    {
        securityContext.assertCredentialsNotExpired();

        ZoneId zoneId = getConfiguredTimeZone();
        List<TransactionStatusResult> result = new ArrayList<>();
        for ( DatabaseContext databaseContext : getDatabaseManager().registeredDatabases().values() )
        {
            if ( databaseContext.database().isStarted() )
            {
                DatabaseScope dbScope = new DatabaseScope( databaseContext.database().getNamedDatabaseId().name() );
                Map<KernelTransactionHandle,Optional<QuerySnapshot>> handleQuerySnapshotsMap = new HashMap<>();
                for ( KernelTransactionHandle tx : getExecutingTransactions( databaseContext ) )
                {
                    String username = tx.subject().username();
                    var action = new AdminActionOnResource( SHOW_TRANSACTION, dbScope, new UserSegment( username ) );
                    if ( isSelfOrAllows( username, action ) )
                    {
                        handleQuerySnapshotsMap.put( tx, tx.executingQuery().map( ExecutingQuery::snapshot ) );
                    }
                }
                TransactionDependenciesResolver transactionBlockerResolvers = new TransactionDependenciesResolver( handleQuerySnapshotsMap );

                for ( KernelTransactionHandle tx : handleQuerySnapshotsMap.keySet() )
                {
                    result.add( new TransactionStatusResult( databaseContext.databaseFacade().databaseName(), tx, transactionBlockerResolvers,
                                                             handleQuerySnapshotsMap, zoneId ) );
                }
            }
        }

        return result.stream();
    }

    @SystemProcedure
    @Description( "Kill transaction with provided id." )
    @Procedure( name = "dbms.killTransaction", mode = DBMS )
    public Stream<TransactionMarkForTerminationResult> killTransaction( @Name( "id" ) String transactionId ) throws InvalidArgumentsException
    {
        requireNonNull( transactionId );
        return killTransactions( singletonList( transactionId ) );
    }

    @SystemProcedure
    @Description( "Kill transactions with provided ids." )
    @Procedure( name = "dbms.killTransactions", mode = DBMS )
    public Stream<TransactionMarkForTerminationResult> killTransactions( @Name( "ids" ) List<String> transactionIds ) throws InvalidArgumentsException
    {
        requireNonNull( transactionIds );
        securityContext.assertCredentialsNotExpired();
        log.warn( "User %s trying to kill transactions: %s.", securityContext.subject().username(), transactionIds.toString() );

        DatabaseManager<DatabaseContext> databaseManager = getDatabaseManager();
        DatabaseIdRepository databaseIdRepository = databaseManager.databaseIdRepository();

        Map<NamedDatabaseId,Set<TransactionId>> byDatabase = new HashMap<>();
        for ( String idText : transactionIds )
        {
            TransactionId id = TransactionId.parse( idText );
            Optional<NamedDatabaseId> namedDatabaseId = databaseIdRepository.getByName( id.database() );
            namedDatabaseId.ifPresent( databaseId -> byDatabase.computeIfAbsent( databaseId, ignore -> new HashSet<>() ).add( id ) );
        }

        Map<String,KernelTransactionHandle> handles = new HashMap<>( transactionIds.size() );
        for ( Map.Entry<NamedDatabaseId,Set<TransactionId>> entry : byDatabase.entrySet() )
        {
            NamedDatabaseId databaseId = entry.getKey();
            var dbScope = new DatabaseScope( databaseId.name() );
            Optional<DatabaseContext> maybeDatabaseContext = databaseManager.getDatabaseContext( databaseId );
            if ( maybeDatabaseContext.isPresent() )
            {
                Set<TransactionId> txIds = entry.getValue();
                DatabaseContext databaseContext = maybeDatabaseContext.get();
                for ( KernelTransactionHandle tx : getExecutingTransactions( databaseContext ) )
                {
                    String username = tx.subject().username();
                    var action = new AdminActionOnResource( TERMINATE_TRANSACTION, dbScope, new UserSegment( username ) );
                    if ( !isSelfOrAllows( username, action ) )
                    {
                        continue;
                    }
                    TransactionId txIdRepresentation = new TransactionId( databaseId.name(), tx.getUserTransactionId() );
                    if ( txIds.contains( txIdRepresentation ) )
                    {
                        handles.put( txIdRepresentation.toString(), tx );
                    }
                }
            }
        }

        return transactionIds.stream().map( id -> terminateTransaction( handles, id ) );
    }

    @SystemProcedure
    @Description( "List all queries currently executing at this instance that are visible to the user." )
    @Procedure( name = "dbms.listQueries", mode = DBMS )
    public Stream<QueryStatusResult> listQueries() throws InvalidArgumentsException
    {
        securityContext.assertCredentialsNotExpired();

        ZoneId zoneId = getConfiguredTimeZone();
        List<QueryStatusResult> result = new ArrayList<>();

        for ( FabricTransaction tx : getFabricTransactions() )
        {
            for ( ExecutingQuery query : getActiveFabricQueries( tx ) )
            {
                String username = query.username();
                var action = new AdminActionOnResource( SHOW_TRANSACTION, ALL, new UserSegment( username ) );
                if ( isSelfOrAllows( username, action ) )
                {
                    result.add( new QueryStatusResult( query, (InternalTransaction) transaction, zoneId, "none" ) );
                }
            }
        }

        for ( DatabaseContext databaseContext : getDatabaseManager().registeredDatabases().values() )
        {
            if ( databaseContext.database().isStarted() )
            {
                DatabaseScope dbScope = new DatabaseScope( databaseContext.database().getNamedDatabaseId().name() );
                for ( KernelTransactionHandle tx : getExecutingTransactions( databaseContext ) )
                {
                    if ( tx.executingQuery().isPresent() )
                    {
                        ExecutingQuery query = tx.executingQuery().get();

                        // Include both the executing query and any previous queries (parent queries of nested query) in the result.
                        while ( query != null )
                        {
                            String username = query.username();
                            var action = new AdminActionOnResource( SHOW_TRANSACTION, dbScope, new UserSegment( username ) );
                            if ( isSelfOrAllows( username, action ) )
                            {
                                result.add(
                                        new QueryStatusResult( query, (InternalTransaction) transaction, zoneId,
                                                databaseContext.databaseFacade().databaseName() ) );
                            }

                            query = query.getPreviousQuery();
                        }
                    }
                }
            }
        }
        return result.stream();
    }

    @SystemProcedure
    @Description( "Kill all transactions executing the query with the given query id." )
    @Procedure( name = "dbms.killQuery", mode = DBMS )
    public Stream<QueryTerminationResult> killQuery( @Name( "id" ) String idText ) throws InvalidArgumentsException
    {
        return killQueries( singletonList( idText ) );
    }

    @SystemProcedure
    @Description( "Kill all transactions executing a query with any of the given query ids." )
    @Procedure( name = "dbms.killQueries", mode = DBMS )
    public Stream<QueryTerminationResult> killQueries( @Name( "ids" ) List<String> idTexts ) throws InvalidArgumentsException
    {
        securityContext.assertCredentialsNotExpired();

        DatabaseManager<DatabaseContext> databaseManager = getDatabaseManager();
        DatabaseIdRepository databaseIdRepository = databaseManager.databaseIdRepository();

        Map<Long,QueryId> queryIds = new HashMap<>( idTexts.size() );
        for ( String idText : idTexts )
        {
            QueryId id = QueryId.parse( idText );
            queryIds.put( id.internalId(), id );
        }

        List<QueryTerminationResult> result = new ArrayList<>( queryIds.size() );

        for ( FabricTransaction tx : getFabricTransactions() )
        {
            for ( ExecutingQuery query : getActiveFabricQueries( tx ) )
            {
                QueryId givenQueryId = queryIds.remove( query.internalQueryId() );
                if ( givenQueryId != null )
                {
                    result.add( killFabricQueryTransaction( givenQueryId, tx, query ) );
                }
            }
        }

        for ( Map.Entry<NamedDatabaseId,DatabaseContext> databaseEntry : databaseManager.registeredDatabases().entrySet() )
        {
            NamedDatabaseId databaseId = databaseEntry.getKey();
            DatabaseContext databaseContext = databaseEntry.getValue();
            if ( databaseContext.database().isStarted() )
            {
                for ( KernelTransactionHandle tx : getExecutingTransactions( databaseContext ) )
                {
                    if ( tx.executingQuery().isPresent() )
                    {
                        QueryId givenQueryId = queryIds.remove( tx.executingQuery().get().internalQueryId() );
                        if ( givenQueryId != null )
                        {
                            result.add( killQueryTransaction( givenQueryId, tx, databaseId ) );
                        }
                    }
                }
            }
        }

        // Add error about the rest
        for ( QueryId queryId : queryIds.values() )
        {
            result.add( new QueryFailedTerminationResult( queryId, "n/a", "No Query found with this id" ) );
        }

        return result.stream();
    }

    @SystemProcedure
    @Description( "List all accepted network connections at this instance that are visible to the user." )
    @Procedure( name = "dbms.listConnections", mode = DBMS )
    public Stream<ListConnectionResult> listConnections()
    {
        securityContext.assertCredentialsNotExpired();

        NetworkConnectionTracker connectionTracker = getConnectionTracker();
        ZoneId timeZone = getConfiguredTimeZone();

        return connectionTracker.activeConnections()
                .stream()
                .filter( connection -> isAdminOrSelf( connection.username() ) )
                .map( connection -> new ListConnectionResult( connection, timeZone ) );
    }

    @SystemProcedure
    @Description( "Kill network connection with the given connection id." )
    @Procedure( name = "dbms.killConnection", mode = DBMS )
    public Stream<ConnectionTerminationResult> killConnection( @Name( "id" ) String id )
    {
        return killConnections( singletonList( id ) );
    }

    @SystemProcedure
    @Description( "Kill all network connections with the given connection ids." )
    @Procedure( name = "dbms.killConnections", mode = DBMS )
    public Stream<ConnectionTerminationResult> killConnections( @Name( "ids" ) List<String> ids )
    {
        securityContext.assertCredentialsNotExpired();

        NetworkConnectionTracker connectionTracker = getConnectionTracker();

        return ids.stream().map( id -> killConnection( id, connectionTracker ) );
    }

    private NetworkConnectionTracker getConnectionTracker()
    {
        return resolver.resolveDependency( NetworkConnectionTracker.class );
    }

    private ConnectionTerminationResult killConnection( String id, NetworkConnectionTracker connectionTracker )
    {
        TrackedNetworkConnection connection = connectionTracker.get( id );
        if ( connection != null )
        {
            if ( isAdminOrSelf( connection.username() ) )
            {
                connection.close();
                return new ConnectionTerminationResult( id, connection.username() );
            }
            throw new AuthorizationViolationException( format("Executing admin procedure is not allowed for %s.", securityContext.description() ) );
        }
        return new ConnectionTerminationFailedResult( id );
    }

    private QueryTerminationResult killQueryTransaction( QueryId queryId, KernelTransactionHandle handle, NamedDatabaseId databaseId )
    {
        Optional<ExecutingQuery> query = handle.executingQuery();
        ExecutingQuery executingQuery = query.orElseThrow( () -> new IllegalStateException( "Query should exist since we filtered based on query ids" ) );
        String username = executingQuery.username();
        var action = new AdminActionOnResource( TERMINATE_TRANSACTION, new DatabaseScope( databaseId.name() ), new UserSegment( username ) );
        if ( isSelfOrAllows( username, action ) )
        {
            if ( handle.isClosing() )
            {
                return new QueryFailedTerminationResult( queryId, username, "Unable to kill queries when underlying transaction is closing." );
            }
            handle.markForTermination( Status.Transaction.Terminated );
            return new QueryTerminationResult( queryId, username, "Query found" );
        }
        else
        {
            throw new AuthorizationViolationException( PERMISSION_DENIED );
        }
    }

    private QueryTerminationResult killFabricQueryTransaction( QueryId queryId, FabricTransaction tx, ExecutingQuery query )
    {
        String username = query.username();
        var action = new AdminActionOnResource( TERMINATE_TRANSACTION, ALL, new UserSegment( username ) );
        if ( isSelfOrAllows( username, action ) )
        {
            tx.markForTermination( Status.Transaction.Terminated );
            return new QueryTerminationResult( queryId, username, "Query found" );
        }
        else
        {
            throw new AuthorizationViolationException( PERMISSION_DENIED );
        }
    }

    private Set<FabricTransaction> getFabricTransactions()
    {
        return getFabricTransactionManager().getOpenTransactions();
    }

    private List<ExecutingQuery> getActiveFabricQueries( FabricTransaction tx )
    {
        return tx.getLastSubmittedStatement().stream()
                .filter( FabricStatementLifecycles.StatementLifecycle::inFabricPhase )
                .map( FabricStatementLifecycles.StatementLifecycle::getMonitoredQuery )
                .collect( toList() );
    }

    private TransactionManager getFabricTransactionManager()
    {
        return resolver.resolveDependency( TransactionManager.class );
    }

    private TransactionMarkForTerminationResult terminateTransaction( Map<String,KernelTransactionHandle> handles, String transactionId )
    {
        KernelTransactionHandle handle = handles.get( transactionId );
        String currentUser = securityContext.subject().username();
        if ( handle == null )
        {
            return new TransactionMarkForTerminationFailedResult( transactionId, currentUser );
        }
        if ( handle.isClosing() )
        {
            return new TransactionMarkForTerminationFailedResult( transactionId, currentUser, "Unable to kill closing transactions." );
        }
        log.debug( "User %s terminated transaction %s.", currentUser, transactionId );
        handle.markForTermination( Status.Transaction.Terminated );
        return new TransactionMarkForTerminationResult( transactionId, handle.subject().username() );
    }

    private static Set<KernelTransactionHandle> getExecutingTransactions( DatabaseContext databaseContext )
    {
        Dependencies dependencies = databaseContext.dependencies();
        if ( dependencies != null )
        {
            return dependencies.resolveDependency( KernelTransactions.class ).executingTransactions();
        }
        else
        {
            return Collections.emptySet();
        }
    }

    private boolean isSelfOrAllows( String username, AdminActionOnResource actionOnResource )
    {
        return securityContext.subject().hasUsername( username ) || securityContext.allowsAdminAction( actionOnResource );
    }

    private boolean isAdminOrSelf( String username )
    {
        return securityContext.allowExecuteAdminProcedure( callContext.id() ) || securityContext.subject().hasUsername( username );
    }

    private GraphDatabaseAPI getSystemDatabase()
    {
        return (GraphDatabaseAPI) graph.getDependencyResolver().resolveDependency( DatabaseManagementService.class ).database( SYSTEM_DATABASE_NAME );
    }

    private StoreIdProvider getSystemDatabaseStoreIdProvider( GraphDatabaseAPI databaseAPI )
    {
        return databaseAPI.getDependencyResolver().resolveDependency( StoreIdProvider.class );
    }

    private DatabaseManager<DatabaseContext> getDatabaseManager()
    {
        return (DatabaseManager<DatabaseContext>) resolver.resolveDependency( DatabaseManager.class );
    }

    private ZoneId getConfiguredTimeZone()
    {
        Config config = graph.getDependencyResolver().resolveDependency( Config.class );
        return config.get( GraphDatabaseSettings.db_timezone ).getZoneId();
    }

    public static class SystemInfo
    {
        public final String id;
        public final String name;
        public final String creationDate;

        public SystemInfo( String id, String name, String creationDate )
        {
            this.id = id;
            this.name = name;
            this.creationDate = creationDate;
        }
    }

    public static class FunctionResult
    {
        public final String name;
        public final String signature;
        public final String category;
        public final String description;
        public final boolean aggregating;
        public final List<String> defaultBuiltInRoles = null; // this is just so that the community version has the same signature as in enterprise

        private FunctionResult( UserFunctionSignature signature, boolean isAggregation )
        {
            this.name = signature.name().toString();
            this.signature = signature.toString();
            this.category = signature.category().orElse( "" );
            this.description = signature.description().orElse( "" );
            this.aggregating = isAggregation;
        }

        private FunctionResult( FunctionInformation info )
        {
            this.name = info.getFunctionName();
            this.signature = info.getSignature();
            this.category = info.getCategory();
            this.description = info.getDescription();
            this.aggregating = info.isAggregationFunction();
        }
    }

    public static class ProcedureResult
    {
        public final String name;
        public final String signature;
        public final String description;
        public final String mode;
        public final List<String> defaultBuiltInRoles = null; // this is just so that the community version has the same signature as in enterprise
        public final boolean worksOnSystem;

        private ProcedureResult( ProcedureSignature signature )
        {
            this.name = signature.name().toString();
            this.signature = signature.toString();
            this.description = signature.description().orElse( "" );
            this.mode = signature.mode().toString();
            this.worksOnSystem = signature.systemProcedure();
        }
    }

    public static class StringResult
    {
        public final String value;

        StringResult( String value )
        {
            this.value = value;
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

    public static class SystemGraphComponentStatusResult
    {
        public final String status;
        public final String description;
        public final String resolution;

        SystemGraphComponentStatusResult( SystemGraphComponent.Status status )
        {
            this.status = status.name();
            this.description = status.description();
            this.resolution = status.resolution();
        }
    }

    public static class SystemGraphComponentUpgradeResult
    {
        public final String status;
        public final String upgradeResult;

        SystemGraphComponentUpgradeResult( String status, String upgradeResult )
        {
            this.status = status;
            this.upgradeResult = upgradeResult;
        }
    }

    public static class QueryTerminationResult
    {
        public final String queryId;
        public final String username;
        public final String message;

        public QueryTerminationResult( QueryId queryId, String username, String message )
        {
            this.queryId = queryId.toString();
            this.username = username;
            this.message = message;
        }
    }

    public static class QueryFailedTerminationResult extends QueryTerminationResult
    {
        public QueryFailedTerminationResult( QueryId queryId, String username, String message )
        {
            super( queryId, username, message );
        }
    }
}
