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
package org.neo4j.kernel.database;

import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.Executor;

import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.configuration.helpers.NormalizedDatabaseName;
import org.neo4j.dbms.api.DatabaseNotFoundException;
import org.neo4j.dbms.database.DatabaseManager;
import org.neo4j.scheduler.Group;
import org.neo4j.scheduler.JobScheduler;

import static org.neo4j.dbms.database.SystemGraphInitializer.DATABASE_LABEL;
import static org.neo4j.dbms.database.SystemGraphInitializer.DATABASE_NAME_PROPERTY;
import static org.neo4j.dbms.database.SystemGraphInitializer.DATABASE_UUID_PROPERTY;

public class SystemDbDatabaseIdRepository implements DatabaseIdRepository
{
    private final DatabaseManager<?> databaseManager;
    private final Executor executor;

    public SystemDbDatabaseIdRepository( DatabaseManager<?> databaseManager, JobScheduler jobScheduler )
    {
        this.databaseManager = databaseManager;
        this.executor = jobScheduler.executor( Group.DATABASE_ID_REPOSITORY );
    }

    @Override
    public Optional<DatabaseId> get( NormalizedDatabaseName normalizedDatabaseName )
    {
        try
        {
            return CompletableFuture.supplyAsync( () -> get0( normalizedDatabaseName ), executor ).join();
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
    private Optional<DatabaseId> get0( NormalizedDatabaseName normalizedDatabaseName )
    {
        var databaseName = normalizedDatabaseName.name();
        var context = databaseManager.getDatabaseContext( SYSTEM_DATABASE_ID )
                .orElseThrow( () -> new DatabaseNotFoundException( GraphDatabaseSettings.SYSTEM_DATABASE_NAME ) );

        var db = context.databaseFacade();
        try ( var tx = db.beginTx() )
        {
            var node = db.findNode( DATABASE_LABEL, DATABASE_NAME_PROPERTY, databaseName );

            if ( node == null )
            {
                return Optional.empty();
            }
            var uuid = node.getProperty( DATABASE_UUID_PROPERTY );
            if ( uuid == null )
            {
                throw new IllegalStateException( "Database has no uuid: " + databaseName );
            }
            if ( !(uuid instanceof String) )
            {
                throw new IllegalStateException( "Database has non String uuid: " + databaseName );
            }
            return Optional.of( new DatabaseId( databaseName, UUID.fromString( (String)uuid ) ) );
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
}
