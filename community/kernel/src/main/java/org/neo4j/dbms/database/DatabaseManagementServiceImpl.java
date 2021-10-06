/*
 * Copyright (c) "Neo4j"
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
package org.neo4j.dbms.database;

import org.eclipse.collections.api.iterator.LongIterator;

import java.util.List;
import java.util.stream.Collectors;

import org.neo4j.configuration.Config;
import org.neo4j.configuration.helpers.DatabaseNameValidator;
import org.neo4j.dbms.api.DatabaseManagementException;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.dbms.api.DatabaseNotFoundException;
import org.neo4j.exceptions.KernelException;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.QueryExecutionException;
import org.neo4j.graphdb.config.Configuration;
import org.neo4j.graphdb.event.DatabaseEventListener;
import org.neo4j.graphdb.event.TransactionEventListener;
import org.neo4j.internal.kernel.api.security.LoginContext;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.api.txstate.TransactionState;
import org.neo4j.kernel.database.NamedDatabaseId;
import org.neo4j.kernel.impl.api.KernelTransactionImplementation;
import org.neo4j.kernel.impl.coreapi.InternalTransaction;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.kernel.internal.event.GlobalTransactionEventListeners;
import org.neo4j.kernel.lifecycle.Lifecycle;
import org.neo4j.kernel.monitoring.DatabaseEventListeners;
import org.neo4j.logging.Log;
import org.neo4j.storageengine.api.txstate.NodeState;
import org.neo4j.token.TokenHolders;
import org.neo4j.values.storable.Values;

import static org.neo4j.configuration.GraphDatabaseInternalSettings.storage_engine;
import static org.neo4j.configuration.GraphDatabaseSettings.SYSTEM_DATABASE_NAME;
import static org.neo4j.dbms.database.TopologyGraphDbmsModel.DATABASE_LABEL;
import static org.neo4j.dbms.database.TopologyGraphDbmsModel.DATABASE_NAME_PROPERTY;
import static org.neo4j.dbms.database.TopologyGraphDbmsModel.DATABASE_STORAGE_ENGINE_PROPERTY;

public class DatabaseManagementServiceImpl implements DatabaseManagementService
{
    private static final SystemDatabaseExecutionContext NO_COMMIT_HOOK = ( db, tx ) -> {};

    private final DatabaseManager<?> databaseManager;
    private final Lifecycle globalLife;
    private final DatabaseEventListeners databaseEventListeners;
    private final GlobalTransactionEventListeners transactionEventListeners;
    private final Log log;
    private final Config globalConfig;

    public DatabaseManagementServiceImpl( DatabaseManager<?> databaseManager, Lifecycle globalLife,
            DatabaseEventListeners databaseEventListeners, GlobalTransactionEventListeners transactionEventListeners, Log log, Config globalConfig )
    {
        this.databaseManager = databaseManager;
        this.globalLife = globalLife;
        this.databaseEventListeners = databaseEventListeners;
        this.transactionEventListeners = transactionEventListeners;
        this.log = log;
        this.globalConfig = globalConfig;
    }

    @Override
    public GraphDatabaseService database( String name ) throws DatabaseNotFoundException
    {
        return databaseManager.getDatabaseContext( name )
                .orElseThrow( () -> new DatabaseNotFoundException( name ) ).databaseFacade();
    }

    @Override
    public void createDatabase( String name, Configuration databaseSpecificSettings )
    {
        String storageEngineName = getStorageEngine( databaseSpecificSettings );
        systemDatabaseExecute( "CREATE DATABASE `" + name + "`", ( database, transaction ) ->
        {
            // Inject the configured storage engine as a property on the node representing the created database
            // This is somewhat a temporary measure before CREATE DATABASE gets support for specifying the storage engine
            // directly into the command syntax.

            TransactionState txState = ((KernelTransactionImplementation) transaction.kernelTransaction()).txState();
            TokenHolders tokenHolders = database.getDependencyResolver().resolveDependency( TokenHolders.class );
            long nodeId = findNodeForCreatedDatabaseInTransactionState( txState, tokenHolders, name );
            int storageEngineNamePropertyKeyTokenId = tokenHolders.propertyKeyTokens().getOrCreateId( DATABASE_STORAGE_ENGINE_PROPERTY );
            txState.nodeDoAddProperty( nodeId, storageEngineNamePropertyKeyTokenId, Values.stringValue( storageEngineName ) );
        } );
    }

    private String getStorageEngine( Configuration databaseSpecificSettings )
    {
        String dbSpecificStorageEngineName = databaseSpecificSettings.get( storage_engine );
        return dbSpecificStorageEngineName != null ? dbSpecificStorageEngineName : globalConfig.get( storage_engine );
    }

    private long findNodeForCreatedDatabaseInTransactionState( TransactionState txState, TokenHolders tokenHolders, String name )
    {
        int databaseLabelTokenId = tokenHolders.labelTokens().getIdByName( DATABASE_LABEL.name() );
        int databaseNamePropertyKeyTokenId = tokenHolders.propertyKeyTokens().getIdByName( DATABASE_NAME_PROPERTY );
        LongIterator addedNodes = txState.addedAndRemovedNodes().getAdded().longIterator();
        while ( addedNodes.hasNext() )
        {
            long nodeId = addedNodes.next();
            NodeState nodeState = txState.getNodeState( nodeId );
            // The database name entered by user goes through the DatabaseNameValidator, which also makes the name lower-case.
            // Use the same validator to end up with the same name to compare with.
            String validatedName = DatabaseNameValidator.validateDatabaseNamePattern( name );
            if ( nodeState.labelDiffSets().isAdded( databaseLabelTokenId ) &&
                    validatedName.equals( nodeState.propertyValue( databaseNamePropertyKeyTokenId ).asObjectCopy().toString() ) )
            {
                return nodeId;
            }
        }
        throw new IllegalStateException( "Couldn't find the node representing the created database '" + name + "'" );
    }

    @Override
    public void dropDatabase( String name )
    {
        systemDatabaseExecute( "DROP DATABASE `" + name + "`" );
    }

    @Override
    public void startDatabase( String name )
    {
        systemDatabaseExecute( "START DATABASE `" + name + "`" );
    }

    @Override
    public void shutdownDatabase( String name )
    {
        systemDatabaseExecute( "STOP DATABASE `" + name + "`" );
    }

    @Override
    public List<String> listDatabases()
    {
        return databaseManager.registeredDatabases().keySet().stream().map( NamedDatabaseId::name ).sorted().collect( Collectors.toList() );
    }

    @Override
    public void registerDatabaseEventListener( DatabaseEventListener listener )
    {
        databaseEventListeners.registerDatabaseEventListener( listener );
    }

    @Override
    public void unregisterDatabaseEventListener( DatabaseEventListener listener )
    {
        databaseEventListeners.unregisterDatabaseEventListener( listener );
    }

    @Override
    public void registerTransactionEventListener( String databaseName, TransactionEventListener<?> listener )
    {
        validateDatabaseName( databaseName );
        transactionEventListeners.registerTransactionEventListener( databaseName, listener );
    }

    @Override
    public void unregisterTransactionEventListener( String databaseName, TransactionEventListener<?> listener )
    {
        transactionEventListeners.unregisterTransactionEventListener( databaseName, listener );
    }

    @Override
    public void shutdown()
    {
        try
        {
            log.info( "Shutdown started" );
            globalLife.shutdown();
        }
        catch ( Exception throwable )
        {
            String message = "Shutdown failed";
            log.error( message, throwable );
            throw new RuntimeException( message, throwable );
        }
    }

    private void systemDatabaseExecute( String query )
    {
        systemDatabaseExecute( query, NO_COMMIT_HOOK );
    }

    private void systemDatabaseExecute( String query, SystemDatabaseExecutionContext beforeCommitHook )
    {
        try
        {
            GraphDatabaseAPI database = (GraphDatabaseAPI) database( SYSTEM_DATABASE_NAME );
            try ( InternalTransaction transaction = database.beginTransaction( KernelTransaction.Type.EXPLICIT, LoginContext.AUTH_DISABLED ) )
            {
                transaction.execute( query );
                beforeCommitHook.accept( database, transaction );
                transaction.commit();
            }
        }
        catch ( QueryExecutionException | KernelException e )
        {
            throw new DatabaseManagementException( e );
        }
    }

    private static void validateDatabaseName( String databaseName )
    {
        if ( SYSTEM_DATABASE_NAME.equals( databaseName ) )
        {
            throw new IllegalArgumentException( "Registration of transaction event listeners on " + SYSTEM_DATABASE_NAME + " is not supported." );
        }
    }

    private interface SystemDatabaseExecutionContext
    {
        void accept( GraphDatabaseAPI database, InternalTransaction transaction ) throws KernelException;
    }
}
