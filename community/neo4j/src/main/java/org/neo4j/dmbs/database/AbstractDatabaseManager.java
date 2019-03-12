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
package org.neo4j.dmbs.database;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.function.BiConsumer;

import org.neo4j.configuration.Config;
import org.neo4j.dbms.database.DatabaseContext;
import org.neo4j.dbms.database.DatabaseManager;
import org.neo4j.graphdb.facade.spi.ClassicCoreSPI;
import org.neo4j.graphdb.factory.module.DatabaseModule;
import org.neo4j.graphdb.factory.module.GlobalModule;
import org.neo4j.graphdb.factory.module.edition.AbstractEditionModule;
import org.neo4j.helpers.Exceptions;
import org.neo4j.kernel.api.procedure.GlobalProcedures;
import org.neo4j.kernel.database.Database;
import org.neo4j.kernel.impl.factory.GraphDatabaseFacade;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;
import org.neo4j.logging.Logger;

import static org.neo4j.configuration.GraphDatabaseSettings.default_database;

public abstract class AbstractDatabaseManager extends LifecycleAdapter implements DatabaseManager
{
    private final GlobalModule globalModule;
    private final AbstractEditionModule edition;
    private final GlobalProcedures globalProcedures;
    private final GraphDatabaseFacade graphDatabaseFacade;
    protected final Logger log;

    public AbstractDatabaseManager( GlobalModule globalModule, AbstractEditionModule edition, GlobalProcedures globalProcedures,
            Logger log, GraphDatabaseFacade graphDatabaseFacade )
    {
        this.log = log;
        this.globalModule = globalModule;
        this.edition = edition;
        this.globalProcedures = globalProcedures;
        this.graphDatabaseFacade = graphDatabaseFacade;
    }

    @Override
    public void start() throws Exception
    {
        forEachDatabase( this::startDatabase );
    }

    @Override
    public void stop() throws Exception
    {
        forEachDatabase( this::stopDatabase );
    }

    @Override
    public List<String> listDatabases()
    {
        ArrayList<String> names = new ArrayList<>( getDatabaseMap().keySet() );
        names.sort( Comparator.naturalOrder() );
        return names;
    }

    protected abstract Map<String,DatabaseContext> getDatabaseMap();

    protected DatabaseContext createNewDatabaseContext( String databaseName )
    {
        log.log( "Creating '%s' database.", databaseName );
        Config globalConfig = globalModule.getGlobalConfig();
        GraphDatabaseFacade facade =
                globalConfig.get( default_database ).equals( databaseName ) ? graphDatabaseFacade : new GraphDatabaseFacade();
        DatabaseModule dataSource = new DatabaseModule( databaseName, globalModule, edition, globalProcedures, facade );
        ClassicCoreSPI spi = new ClassicCoreSPI( globalModule, dataSource, log, dataSource.coreAPIAvailabilityGuard, edition.getThreadToTransactionBridge() );
        Database database = dataSource.database;
        facade.init( spi, edition.getThreadToTransactionBridge(), globalConfig, database.getTokenHolders() );
        return new DatabaseContext( database, facade );
    }

    private void forEachDatabase( BiConsumer<String, DatabaseContext> consumer ) throws Exception
    {
        Throwable error = null;
        for ( Map.Entry<String,DatabaseContext> databaseContextEntry : getDatabaseMap().entrySet() )
        {
            try
            {
                consumer.accept( databaseContextEntry.getKey(), databaseContextEntry.getValue() );
            }
            catch ( Throwable t )
            {
                error = Exceptions.chain( error, t );
            }
        }
        if ( error instanceof Exception )
        {
            throw (Exception) error;
        }
        else if ( error != null )
        {
            throw new Exception( error );
        }
    }

    protected void startDatabase( String databaseName, DatabaseContext context )
    {
        log.log( "Starting '%s' database.", databaseName );
        Database database = context.getDatabase();
        database.start();
    }

    protected void stopDatabase( String databaseName, DatabaseContext context )
    {
        log.log( "Stop '%s' database.", databaseName );
        Database database = context.getDatabase();
        database.stop();
    }
}
