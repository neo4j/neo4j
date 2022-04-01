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

import org.neo4j.dbms.api.DatabaseExistsException;
import org.neo4j.dbms.api.DatabaseManagementException;
import org.neo4j.dbms.api.DatabaseNotFoundException;
import org.neo4j.graphdb.factory.module.GlobalModule;
import org.neo4j.kernel.database.NamedDatabaseId;

import static java.util.Objects.requireNonNull;
import static org.neo4j.configuration.GraphDatabaseSettings.default_database;
import static org.neo4j.kernel.database.NamedDatabaseId.NAMED_SYSTEM_DATABASE_ID;

public final class DefaultDatabaseManager extends AbstractDatabaseManager<StandaloneDatabaseContext>
{
    private DatabaseContextFactory<StandaloneDatabaseContext> databaseContextFactory;

    public DefaultDatabaseManager( GlobalModule globalModule, DatabaseContextFactory<StandaloneDatabaseContext> databaseContextFactory )
    {
        super( globalModule, true );
        this.databaseContextFactory = databaseContextFactory;
    }

    @Override
    public void initialiseSystemDatabase()
    {
        createDatabase( NAMED_SYSTEM_DATABASE_ID );
    }

    @Override
    public void initialiseDefaultDatabase()
    {
        String databaseName = config.get( default_database );
        NamedDatabaseId namedDatabaseId = databaseIdRepository().getByName( databaseName )
                .orElseThrow( () -> new DatabaseNotFoundException( "Default database not found: " + databaseName ) );
        StandaloneDatabaseContext context = createDatabase( namedDatabaseId );
        if ( manageDatabasesOnStartAndStop )
        {
            this.startDatabase( namedDatabaseId, context );
        }
    }

    /**
     * Create database with specified name.
     * Database name should be unique.
     * By default a database is in a started state when it is initially created.
     * @param namedDatabaseId ID of database to create
     * @throws DatabaseExistsException In case if database with specified name already exists
     * @return database context for newly created database
     */
    @Override
    public synchronized StandaloneDatabaseContext createDatabase( NamedDatabaseId namedDatabaseId )
    {
        requireNonNull( namedDatabaseId );
        log.info( "Creating '%s'.", namedDatabaseId );
        checkDatabaseLimit( namedDatabaseId );
        StandaloneDatabaseContext databaseContext = databaseContextFactory.create( namedDatabaseId, DatabaseOptions.SINGLE );
        databaseMap.put( namedDatabaseId, databaseContext );
        return databaseContext;
    }

    @Override
    public void dropDatabase( NamedDatabaseId ignore )
    {
        throw new DatabaseManagementException( "Default database manager does not support database drop." );
    }

    @Override
    public void stopDatabase( NamedDatabaseId ignore )
    {
        throw new DatabaseManagementException( "Default database manager does not support database stop." );
    }

    @Override
    public void startDatabase( NamedDatabaseId namedDatabaseId )
    {
        throw new DatabaseManagementException( "Default database manager does not support starting databases." );
    }

    @Override
    protected void stopDatabase( NamedDatabaseId namedDatabaseId, StandaloneDatabaseContext context )
    {
        try
        {
            super.stopDatabase( namedDatabaseId, context );
        }
        catch ( Throwable t )
        {
            log.error( "Failed to stop " + namedDatabaseId, t );
            context.fail( t );
        }
    }

    @Override
    protected void startDatabase( NamedDatabaseId namedDatabaseId, StandaloneDatabaseContext context )
    {
        try
        {
            super.startDatabase( namedDatabaseId, context );
        }
        catch ( Throwable t )
        {
            log.error( "Failed to start " + namedDatabaseId, t );
            context.fail( t );
        }
    }

    private void checkDatabaseLimit( NamedDatabaseId namedDatabaseId )
    {
        if ( databaseMap.size() >= 2 )
        {
            throw new DatabaseManagementException( "Default database already exists. Fail to create another: " + namedDatabaseId );
        }
    }
}
