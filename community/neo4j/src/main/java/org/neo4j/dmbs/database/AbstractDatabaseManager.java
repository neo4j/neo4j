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

import java.util.Collections;
import java.util.SortedMap;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.function.BiConsumer;

import org.neo4j.configuration.Config;
import org.neo4j.dbms.database.DatabaseContext;
import org.neo4j.dbms.database.DatabaseManager;
import org.neo4j.graphdb.facade.spi.ClassicCoreSPI;
import org.neo4j.graphdb.factory.module.DatabaseModule;
import org.neo4j.graphdb.factory.module.GlobalModule;
import org.neo4j.graphdb.factory.module.edition.AbstractEditionModule;
import org.neo4j.kernel.database.Database;
import org.neo4j.kernel.database.DatabaseId;
import org.neo4j.kernel.impl.factory.GraphDatabaseFacade;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;
import org.neo4j.logging.Log;

import static org.neo4j.configuration.GraphDatabaseSettings.default_database;

public abstract class AbstractDatabaseManager<T extends DatabaseContext> extends LifecycleAdapter implements DatabaseManager<T>
{

    protected final ConcurrentSkipListMap<DatabaseId,T> databaseMap;
    private final GlobalModule globalModule;
    private final AbstractEditionModule edition;
    private final GraphDatabaseFacade graphDatabaseFacade;
    protected final Log log;

    protected AbstractDatabaseManager( GlobalModule globalModule, AbstractEditionModule edition, Log log, GraphDatabaseFacade graphDatabaseFacade )
    {
        this.log = log;
        this.globalModule = globalModule;
        this.edition = edition;
        this.graphDatabaseFacade = graphDatabaseFacade;
        this.databaseMap = new ConcurrentSkipListMap<>();
    }

    @Override
    public void start() throws Exception
    {
        forEachDatabase( databaseMap, this::startDatabase );
    }

    @Override
    public void stop() throws Exception
    {
        //We want to reverse databases in the opposite order to which they were started.
        // Amongst other things this helps to ensure that the system database is stopped last.
        forEachDatabase( databaseMap.descendingMap(), this::stopDatabase );
    }

    public final SortedMap<DatabaseId,T> registeredDatabases()
    {
        return Collections.unmodifiableSortedMap( databaseMap );
    }

    protected T createNewDatabaseContext( DatabaseId databaseId )
    {
        log.info( "Creating '%s' database.", databaseId.name() );
        Config globalConfig = globalModule.getGlobalConfig();
        GraphDatabaseFacade facade =
                globalConfig.get( default_database ).equals( databaseId.name() ) ? graphDatabaseFacade : new GraphDatabaseFacade();
        DatabaseModule dataSource = new DatabaseModule( databaseId, globalModule, edition, facade );
        ClassicCoreSPI spi = new ClassicCoreSPI( globalModule, dataSource, log, dataSource.coreAPIAvailabilityGuard, edition.getThreadToTransactionBridge() );
        Database database = dataSource.database;
        facade.init( spi, edition.getThreadToTransactionBridge(), globalConfig, database.getTokenHolders() );
        return databaseContextFactory( database, facade );
    }

    protected abstract T databaseContextFactory( Database database, GraphDatabaseFacade facade );

    protected final void forEachDatabase( ConcurrentNavigableMap<DatabaseId,T> dbMap, BiConsumer<DatabaseId,T> consumer )
    {
        for ( var databaseContextEntry : dbMap.entrySet() )
        {
            T context = databaseContextEntry.getValue();
            DatabaseId contextKey = databaseContextEntry.getKey();
            try
            {
                consumer.accept( contextKey, context );
            }
            catch ( Throwable t )
            {
                context.fail( t );
                log.error( "Fail to perform operation with a database " + contextKey, t );
            }
        }
    }

    protected void startDatabase( DatabaseId databaseId, T context )
    {
        log.info( "Starting '%s' database.", databaseId.name() );
        Database database = context.database();
        database.start();
    }

    protected void stopDatabase( DatabaseId databaseId, T context )
    {
        log.info( "Stop '%s' database.", databaseId.name() );
        Database database = context.database();
        database.stop();
    }
}
