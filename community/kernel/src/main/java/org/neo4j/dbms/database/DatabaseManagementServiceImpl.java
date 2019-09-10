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
package org.neo4j.dbms.database;

import java.util.List;
import java.util.stream.Collectors;

import org.neo4j.dbms.api.DatabaseManagementException;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.dbms.api.DatabaseNotFoundException;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.QueryExecutionException;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.event.DatabaseEventListener;
import org.neo4j.graphdb.event.TransactionEventListener;
import org.neo4j.kernel.availability.CompositeDatabaseAvailabilityGuard;
import org.neo4j.kernel.database.NamedDatabaseId;
import org.neo4j.kernel.internal.event.GlobalTransactionEventListeners;
import org.neo4j.kernel.lifecycle.Lifecycle;
import org.neo4j.logging.Log;
import org.neo4j.monitoring.DatabaseEventListeners;

import static org.neo4j.configuration.GraphDatabaseSettings.SYSTEM_DATABASE_NAME;

public class DatabaseManagementServiceImpl implements DatabaseManagementService
{
    private final DatabaseManager<?> databaseManager;
    private final CompositeDatabaseAvailabilityGuard globalAvailabilityGuard;
    private final Lifecycle globalLife;
    private final DatabaseEventListeners databaseEventListeners;
    private final GlobalTransactionEventListeners transactionEventListeners;
    private final Log log;

    public DatabaseManagementServiceImpl( DatabaseManager<?> databaseManager, CompositeDatabaseAvailabilityGuard globalAvailabilityGuard, Lifecycle globalLife,
            DatabaseEventListeners databaseEventListeners, GlobalTransactionEventListeners transactionEventListeners, Log log )
    {
        this.databaseManager = databaseManager;
        this.globalAvailabilityGuard = globalAvailabilityGuard;
        this.globalLife = globalLife;
        this.databaseEventListeners = databaseEventListeners;
        this.transactionEventListeners = transactionEventListeners;
        this.log = log;
    }

    @Override
    public GraphDatabaseService database( String name ) throws DatabaseNotFoundException
    {
        return databaseManager.getDatabaseContext( name )
                .orElseThrow( () -> new DatabaseNotFoundException( name ) ).databaseFacade();
    }

    @Override
    public void createDatabase( String name )
    {
        systemDatabaseExecute( "CREATE DATABASE `" + name + "`" );
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
            globalAvailabilityGuard.shutdown();
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
        try
        {
            GraphDatabaseService database = database( SYSTEM_DATABASE_NAME );
            try ( Transaction transaction = database.beginTx() )
            {
                transaction.execute( query );
                transaction.commit();
            }
        }
        catch ( QueryExecutionException e )
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
}
