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

import java.util.NavigableMap;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BiConsumer;

import org.neo4j.configuration.Config;
import org.neo4j.graphdb.factory.module.GlobalModule;
import org.neo4j.graphdb.factory.module.ModularDatabaseCreationContext;
import org.neo4j.graphdb.factory.module.edition.AbstractEditionModule;
import org.neo4j.graphdb.factory.module.edition.context.EditionDatabaseComponents;
import org.neo4j.kernel.api.procedure.GlobalProcedures;
import org.neo4j.kernel.database.Database;
import org.neo4j.kernel.database.DatabaseCreationContext;
import org.neo4j.kernel.database.DatabaseId;
import org.neo4j.kernel.impl.coreapi.CoreAPIAvailabilityGuard;
import org.neo4j.kernel.impl.factory.GraphDatabaseFacade;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;
import org.neo4j.logging.Log;

import static java.util.Collections.unmodifiableNavigableMap;
import static org.neo4j.configuration.GraphDatabaseSettings.default_database;

public abstract class AbstractDatabaseManager<T extends DatabaseContext> extends LifecycleAdapter implements DatabaseManager<T>
{
    protected final ConcurrentHashMap<DatabaseId,T> databaseMap;
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
        this.databaseMap = new ConcurrentHashMap<>();
    }

    @Override
    public void start() throws Exception
    {
        startAllDatabases();
    }

    protected void startAllDatabases()
    {
        forEachDatabase( this::startDatabase, false );
    }

    @Override
    public void stop() throws Exception
    {
        stopAllDatabases();
    }

    private void stopAllDatabases()
    {
        forEachDatabase( this::stopDatabase, true );
    }

    public final SortedMap<DatabaseId,T> registeredDatabases()
    {
        return databasesSnapshot();
    }

    private NavigableMap<DatabaseId,T> databasesSnapshot()
    {
        return unmodifiableNavigableMap( new TreeMap<>( databaseMap ) );
    }

    protected T createNewDatabaseContext( DatabaseId databaseId )
    {
        log.info( "Creating '%s' database.", databaseId.name() );
        Config globalConfig = globalModule.getGlobalConfig();
        GraphDatabaseFacade facade =
                globalConfig.get( default_database ).equals( databaseId.name() ) ? graphDatabaseFacade : new GraphDatabaseFacade();

        DatabaseCreationContext databaseCreationContext = newDatabaseCreationContext( databaseId, facade );
        Database database = new Database( databaseCreationContext );

        CoreAPIAvailabilityGuard coreAPIAvailabilityGuard = new CoreAPIAvailabilityGuard( globalModule.getGlobalAvailabilityGuard(),
                edition.getTransactionStartTimeout() );

        facade.init( database, edition.getThreadToTransactionBridge(), globalConfig, globalModule.getDatabaseInfo(), coreAPIAvailabilityGuard );
        return createDatabaseContext( database, facade );
    }

    protected abstract T createDatabaseContext( Database database, GraphDatabaseFacade facade );

    protected final void forEachDatabase( BiConsumer<DatabaseId,T> consumer, boolean systemDatabaseLast )
    {
        var snapshot = systemDatabaseLast ? databasesSnapshot().descendingMap().entrySet() : databasesSnapshot().entrySet();

        for ( var entry : snapshot )
        {
            DatabaseId databaseId = entry.getKey();
            T context = entry.getValue();
            try
            {
                consumer.accept( databaseId, context );
            }
            catch ( Throwable t )
            {
                context.fail( t );
                log.error( "Failed to perform operation with database " + databaseId, t );
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

    private DatabaseCreationContext newDatabaseCreationContext( DatabaseId databaseId, GraphDatabaseFacade facade )
    {
        EditionDatabaseComponents editionDatabaseComponents = edition.createDatabaseComponents( databaseId );
        GlobalProcedures globalProcedures = edition.getGlobalProcedures();
        return new ModularDatabaseCreationContext( databaseId, globalModule, editionDatabaseComponents, globalProcedures, facade );
    }
}
