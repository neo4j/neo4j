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
package org.neo4j.dmbs.database;

import java.util.Collections;
import java.util.List;
import java.util.Optional;

import org.neo4j.dbms.database.DatabaseManager;
import org.neo4j.graphdb.facade.spi.ClassicCoreSPI;
import org.neo4j.graphdb.factory.module.DataSourceModule;
import org.neo4j.graphdb.factory.module.PlatformModule;
import org.neo4j.graphdb.factory.module.edition.AbstractEditionModule;
import org.neo4j.kernel.impl.factory.GraphDatabaseFacade;
import org.neo4j.kernel.impl.proc.Procedures;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;
import org.neo4j.logging.Logger;

import static org.neo4j.util.Preconditions.checkState;

public final class DefaultDatabaseManager extends LifecycleAdapter implements DatabaseManager
{
    private GraphDatabaseFacade database;
    private final PlatformModule platform;
    private final AbstractEditionModule edition;
    private final Procedures procedures;
    private final Logger log;
    private final GraphDatabaseFacade graphDatabaseFacade;
    private String databaseName;

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
    public Optional<GraphDatabaseFacade> getDatabaseFacade( String name )
    {
        return Optional.ofNullable( database );
    }

    @Override
    public GraphDatabaseFacade createDatabase( String databaseName )
    {
        checkState( database == null, "Database is already created, fail to create another one." );
        log.log( "Creating '%s' database.", databaseName );
        DataSourceModule dataSource = new DataSourceModule( databaseName, platform, edition, procedures, graphDatabaseFacade );
        ClassicCoreSPI spi = new ClassicCoreSPI( platform, dataSource, log, dataSource.getCoreAPIAvailabilityGuard(), edition.getThreadToTransactionBridge() );
        graphDatabaseFacade.init( spi, edition.getThreadToTransactionBridge(), platform.config, dataSource.neoStoreDataSource.getTokenHolders() );
        platform.dataSourceManager.register( dataSource.neoStoreDataSource );
        database = graphDatabaseFacade;
        this.databaseName = databaseName;
        return database;
    }

    @Override
    public void shutdownDatabase( String ignore )
    {
        shutdownDatabase();
    }

    @Override
    public void stop()
    {
        shutdownDatabase();
    }

    private void shutdownDatabase()
    {
        if ( database != null )
        {
            log.log( "Shutting down '%s' database.", database.databaseLayout().getDatabaseName() );
            database.shutdown();
        }
    }

    @Override
    public List<String> listDatabases()
    {
        return databaseName == null ? Collections.emptyList() : Collections.singletonList( databaseName );
    }
}
