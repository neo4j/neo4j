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

import java.util.Optional;

import org.neo4j.dbms.database.DatabaseManager;
import org.neo4j.graphdb.facade.spi.ClassicCoreSPI;
import org.neo4j.graphdb.factory.module.DataSourceModule;
import org.neo4j.graphdb.factory.module.EditionModule;
import org.neo4j.graphdb.factory.module.PlatformModule;
import org.neo4j.kernel.impl.factory.GraphDatabaseFacade;
import org.neo4j.kernel.impl.proc.Procedures;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;
import org.neo4j.logging.Logger;

public final class DefaultDatabaseManager extends LifecycleAdapter implements DatabaseManager
{
    private GraphDatabaseFacade database;
    private final PlatformModule platform;
    private final EditionModule edition;
    private final Procedures procedures;
    private final Logger msgLog;
    private final GraphDatabaseFacade graphDatabaseFacade;

    public DefaultDatabaseManager( PlatformModule platform, EditionModule edition, Procedures procedures,
            Logger msgLog, GraphDatabaseFacade graphDatabaseFacade )
    {
        this.platform = platform;
        this.edition = edition;
        this.procedures = procedures;
        this.msgLog = msgLog;
        this.graphDatabaseFacade = graphDatabaseFacade;
    }

    @Override
    public Optional<GraphDatabaseFacade> getDatabaseFacade( String name )
    {
        return Optional.ofNullable( database );
    }

    @Override
    public GraphDatabaseFacade createDatabase( String name )
    {
        if ( database == null )
        {
            DataSourceModule dataSource = new DataSourceModule( name, platform, edition, procedures, graphDatabaseFacade );
            ClassicCoreSPI spi = new ClassicCoreSPI( platform, dataSource, msgLog, edition.coreAPIAvailabilityGuard );
            graphDatabaseFacade.init( spi, dataSource.threadToTransactionBridge, platform.config, dataSource.tokenHolders );
            database = graphDatabaseFacade;
            return database;
        }
        else
        {
            throw new IllegalStateException( "Database is already created, fail to create another one." );
        }
    }

    @Override
    public synchronized void shutdownDatabase( String name )
    {
        if ( database != null )
        {
            database.shutdown();
        }
    }

    @Override
    public void stop()
    {
        shutdownDatabase( DatabaseManager.DEFAULT_DATABASE_NAME );
    }
}
