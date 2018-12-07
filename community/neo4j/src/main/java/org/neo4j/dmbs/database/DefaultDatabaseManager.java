/*
 * Copyright (c) 2002-2018 "Neo4j,"
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
package org.neo4j.dmbs.database;

import java.util.List;
import java.util.Optional;

import org.neo4j.dbms.database.DatabaseContext;
import org.neo4j.dbms.database.DatabaseManager;
import org.neo4j.graphdb.facade.spi.ClassicCoreSPI;
import org.neo4j.graphdb.factory.module.DatabaseModule;
import org.neo4j.graphdb.factory.module.PlatformModule;
import org.neo4j.graphdb.factory.module.edition.AbstractEditionModule;
import org.neo4j.kernel.database.Database;
import org.neo4j.kernel.impl.factory.GraphDatabaseFacade;
import org.neo4j.kernel.impl.proc.Procedures;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;
import org.neo4j.logging.Logger;

import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static org.neo4j.util.Preconditions.checkState;

public final class DefaultDatabaseManager extends LifecycleAdapter implements DatabaseManager
{
    private DatabaseContext databaseContext;
    private final PlatformModule platform;
    private final AbstractEditionModule edition;
    private final Procedures procedures;
    private final Logger log;
    private final GraphDatabaseFacade graphDatabaseFacade;

    public DefaultDatabaseManager( PlatformModule platform, AbstractEditionModule edition, Procedures procedures,
            Logger log, GraphDatabaseFacade graphDatabaseFacade )
    {
        this.platform = platform;
        this.edition = edition;
        this.procedures = procedures;
        this.log = log;
        this.graphDatabaseFacade = graphDatabaseFacade;
    }

    @Override
    public Optional<DatabaseContext> getDatabaseContext( String name )
    {
        return Optional.ofNullable( databaseContext );
    }

    @Override
    public DatabaseContext createDatabase( String databaseName )
    {
        checkState( databaseContext == null, "Database is already created, fail to create another one." );
        log.log( "Creating '%s' database.", databaseName );
        DatabaseModule databaseModule = new DatabaseModule( databaseName, platform, edition, procedures, graphDatabaseFacade );
        Database database = databaseModule.database;
        ClassicCoreSPI spi =
                new ClassicCoreSPI( platform, databaseModule, log, databaseModule.getCoreAPIAvailabilityGuard(), edition.getThreadToTransactionBridge() );
        graphDatabaseFacade.init( spi, edition.getThreadToTransactionBridge(), platform.config, database.getTokenHolders() );

        databaseContext = new DatabaseContext( database, graphDatabaseFacade );
        return databaseContext;
    }

    @Override
    public void dropDatabase( String ignore )
    {
        throw new UnsupportedOperationException( "Default database manager does not support database drop." );
    }

    @Override
    public void stopDatabase( String ignore )
    {
        stopDatabase();
    }

    @Override
    public void startDatabase( String databaseName )
    {
        throw new UnsupportedOperationException( "Default database manager does not support starting databases." );
    }

    @Override
    public void start()
    {
        if ( databaseContext != null )
        {
            databaseContext.getDatabase().start();
        }
    }

    @Override
    public void stop()
    {
        if ( databaseContext != null )
        {
            databaseContext.getDatabase().stop();
        }
    }

    @Override
    public void shutdown()
    {
        stopDatabase();
    }

    @Override
    public List<String> listDatabases()
    {
        if ( databaseContext == null )
        {
            return emptyList();
        }
        return singletonList( databaseContext.getDatabase().getDatabaseName() );
    }

    private void stopDatabase()
    {
        if ( databaseContext != null )
        {
            Database database = databaseContext.getDatabase();
            log.log( "Shutting down '%s' database.", database.getDatabaseName() );
            database.stop();
            databaseContext = null;
        }
    }
}
