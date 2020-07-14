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
package org.neo4j.kernel.database;

import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.Executor;
import java.util.function.Supplier;

import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.configuration.helpers.NormalizedDatabaseName;
import org.neo4j.dbms.api.DatabaseNotFoundException;
import org.neo4j.dbms.database.DatabaseManager;
import org.neo4j.graphdb.Node;
import org.neo4j.scheduler.Group;
import org.neo4j.scheduler.JobMonitoringParams;
import org.neo4j.scheduler.JobScheduler;
import org.neo4j.scheduler.MonitoredJobExecutor;

import static org.neo4j.dbms.database.SystemGraphDbmsModel.DATABASE_LABEL;
import static org.neo4j.dbms.database.SystemGraphDbmsModel.DATABASE_NAME_PROPERTY;
import static org.neo4j.dbms.database.SystemGraphDbmsModel.DATABASE_UUID_PROPERTY;

public class SystemDbDatabaseIdRepository implements DatabaseIdRepository
{
    private final DatabaseManager<?> databaseManager;
    private final MonitoredJobExecutor monitoredJobExecutor;

    public SystemDbDatabaseIdRepository( DatabaseManager<?> databaseManager, JobScheduler jobScheduler )
    {
        this.databaseManager = databaseManager;
        this.monitoredJobExecutor = jobScheduler.monitoredJobExecutor( Group.DATABASE_ID_REPOSITORY );
    }

    @Override
    public Optional<NamedDatabaseId> getByName( NormalizedDatabaseName normalizedDatabaseName )
    {
        var databaseName = normalizedDatabaseName.name();
        return runAsync( () -> get( DATABASE_NAME_PROPERTY, databaseName ), databaseName );
    }

    @Override
    public Optional<NamedDatabaseId> getById( DatabaseId databaseId )
    {
        return runAsync( () -> get( DATABASE_UUID_PROPERTY, databaseId.uuid().toString() ), null );
    }

    private <T> T runAsync( Supplier<T> supplier, String databaseName )
    {
        try
        {
            var monitoringParams = JobMonitoringParams.systemJob( databaseName, "Obtaining a database ID from System database" );
            Executor executor = job -> monitoredJobExecutor.execute( monitoringParams, job );
            return CompletableFuture.supplyAsync( supplier, executor ).join();
        }
        catch ( CompletionException e )
        {
            if ( e.getCause() instanceof RuntimeException )
            {
                throw (RuntimeException) e.getCause();
            }
            throw e;
        }
    }

    // Run on another thread to avoid running in an enclosing transaction on a different database
    private Optional<NamedDatabaseId> get( String propertyKey, String propertyValue )
    {
        var context = databaseManager.getDatabaseContext( NAMED_SYSTEM_DATABASE_ID ).orElseThrow(
                () -> new DatabaseNotFoundException( GraphDatabaseSettings.SYSTEM_DATABASE_NAME ) );

        var db = context.databaseFacade();
        try ( var tx = db.beginTx() )
        {
            var node = tx.findNode( DATABASE_LABEL, propertyKey, propertyValue );

            if ( node == null )
            {
                return Optional.empty();
            }

            var databaseName = getPropertyOnNode( node, DATABASE_NAME_PROPERTY, propertyValue );
            var databaseUuid = getPropertyOnNode( node, DATABASE_UUID_PROPERTY, propertyValue );

            return Optional.of( new NamedDatabaseId( databaseName, UUID.fromString( databaseUuid ) ) );
        }
        catch ( RuntimeException e )
        {
            throw e;
        }
        catch ( Exception e )
        {
            throw new RuntimeException( e );
        }
    }

    private String getPropertyOnNode( Node node, String key, String database )
    {
        var value = node.getProperty( key );
        if ( value == null )
        {
            throw new IllegalStateException( String.format( "Database has no %s: %s.", key, database ) );
        }
        if ( !(value instanceof String) )
        {
            throw new IllegalStateException( String.format( "Database has non String %s: %s.", key, database ) );
        }
        return (String) value;
    }
}
