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

import java.util.Map;
import java.util.NavigableMap;
import java.util.Optional;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BiConsumer;
import java.util.function.Predicate;

import org.neo4j.collection.Dependencies;
import org.neo4j.configuration.Config;
import org.neo4j.configuration.DatabaseConfig;
import org.neo4j.dbms.api.DatabaseManagementException;
import org.neo4j.graphdb.DatabaseShutdownException;
import org.neo4j.graphdb.factory.module.GlobalModule;
import org.neo4j.graphdb.factory.module.ModularDatabaseCreationContext;
import org.neo4j.graphdb.factory.module.edition.AbstractEditionModule;
import org.neo4j.graphdb.factory.module.edition.CommunityEditionModule;
import org.neo4j.graphdb.factory.module.edition.context.EditionDatabaseComponents;
import org.neo4j.internal.helpers.Exceptions;
import org.neo4j.io.pagecache.context.EmptyVersionContextSupplier;
import org.neo4j.io.pagecache.context.VersionContextSupplier;
import org.neo4j.kernel.api.procedure.GlobalProcedures;
import org.neo4j.kernel.database.Database;
import org.neo4j.kernel.database.DatabaseCreationContext;
import org.neo4j.kernel.database.DatabaseId;
import org.neo4j.kernel.database.DatabaseIdRepository;
import org.neo4j.kernel.database.DatabaseTracers;
import org.neo4j.kernel.database.MapCachingDatabaseIdRepository;
import org.neo4j.kernel.database.NamedDatabaseId;
import org.neo4j.kernel.database.SystemGraphDatabaseIdRepository;
import org.neo4j.kernel.impl.api.LeaseService;
import org.neo4j.kernel.impl.context.TransactionVersionContextSupplier;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;
import org.neo4j.monitoring.Monitors;

import static java.lang.String.format;
import static java.util.Collections.unmodifiableNavigableMap;
import static org.neo4j.configuration.GraphDatabaseInternalSettings.snapshot_query;

public abstract class AbstractDatabaseManager<DB extends DatabaseContext> extends LifecycleAdapter implements DatabaseManager<DB>
{
    protected final Map<NamedDatabaseId,DB> databaseMap;
    protected final GlobalModule globalModule;
    protected final AbstractEditionModule edition;
    protected final Log log;
    protected final boolean manageDatabasesOnStartAndStop;
    protected final Config config;
    protected final LogProvider logProvider;
    private final DatabaseIdRepository.Caching databaseIdRepository;

    protected AbstractDatabaseManager( GlobalModule globalModule, AbstractEditionModule edition, boolean manageDatabasesOnStartAndStop )
    {
        this.logProvider = globalModule.getLogService().getInternalLogProvider();
        this.log = logProvider.getLog( getClass() );
        this.globalModule = globalModule;
        this.config = globalModule.getGlobalConfig();
        this.edition = edition;
        this.manageDatabasesOnStartAndStop = manageDatabasesOnStartAndStop;
        this.databaseMap = new ConcurrentHashMap<>();
        this.databaseIdRepository = createDatabaseIdRepository( globalModule );
    }

    private DatabaseIdRepository.Caching createDatabaseIdRepository( GlobalModule globalModule )
    {
        return CommunityEditionModule.tryResolveOrCreate(
                DatabaseIdRepository.Caching.class,
                globalModule.getExternalDependencyResolver(),
                () -> new MapCachingDatabaseIdRepository(
                        new SystemGraphDatabaseIdRepository( this::getSystemDatabaseContext ) ) );
    }

    @Override
    public DatabaseIdRepository.Caching databaseIdRepository()
    {
        return databaseIdRepository;
    }

    @Override
    public final void init()
    { //no-op
    }

    @Override
    public void start() throws Exception
    {
        if ( manageDatabasesOnStartAndStop )
        {
            startAllDatabases();
        }
    }

    private void startAllDatabases()
    {
        forEachDatabase( this::startDatabase, false, "start" );
    }

    @Override
    public void stop() throws Exception
    {
        if ( manageDatabasesOnStartAndStop )
        {
            stopAllDatabases();
        }
    }

    private void stopAllDatabases()
    {
        forEachDatabase( this::stopDatabase, true, "stop" );
    }

    @Override
    public Optional<DB> getDatabaseContext( NamedDatabaseId namedDatabaseId )
    {
        return Optional.ofNullable( databaseMap.get( namedDatabaseId ) );
    }

    @Override
    public Optional<DB> getDatabaseContext( String databaseName )
    {
        try
        {
            return databaseIdRepository().getByName( databaseName ).flatMap( this::getDatabaseContext );
        }
        catch ( DatabaseShutdownException e )
        {
            return searchContext( id -> id.name().equals( databaseName ) );
        }
    }

    @Override
    public Optional<DB> getDatabaseContext( DatabaseId databaseId )
    {
        try
        {
            return databaseIdRepository().getById( databaseId ).flatMap( this::getDatabaseContext );
        }
        catch ( DatabaseShutdownException e )
        {
            return searchContext( id -> id.databaseId().equals( databaseId ) );
        }
    }

    @Override
    public final SortedMap<NamedDatabaseId,DB> registeredDatabases()
    {
        return databasesSnapshot();
    }

    private NavigableMap<NamedDatabaseId,DB> databasesSnapshot()
    {
        return unmodifiableNavigableMap( new TreeMap<>( databaseMap ) );
    }

    private Optional<DB> searchContext( Predicate<NamedDatabaseId> predicate )
    {
        return databaseMap.entrySet().stream().filter( entry -> predicate.test( entry.getKey() ) ).map( Map.Entry::getValue ).findFirst();
    }

    protected abstract DB createDatabaseContext( NamedDatabaseId namedDatabaseId, DatabaseOptions databaseOptions ) throws Exception;

    protected ModularDatabaseCreationContext newDatabaseCreationContext( NamedDatabaseId namedDatabaseId, DatabaseOptions databaseOptions,
            Dependencies parentDependencies, Monitors parentMonitors )
    {
        EditionDatabaseComponents editionDatabaseComponents = edition.createDatabaseComponents( namedDatabaseId );
        var globalProcedures = parentDependencies.resolveDependency( GlobalProcedures.class );
        var databaseConfig = new DatabaseConfig( databaseOptions.settings(), config, namedDatabaseId );
        var storageEngineFactory = DatabaseCreationContext.selectStorageEngine( globalModule.getFileSystem(), globalModule.getNeo4jLayout(),
                globalModule.getPageCache(), databaseConfig, namedDatabaseId );
        var tracers = new DatabaseTracers( globalModule.getTracers() );

        return new ModularDatabaseCreationContext( namedDatabaseId, globalModule, parentDependencies, parentMonitors,
                                                   editionDatabaseComponents, globalProcedures, createVersionContextSupplier( databaseConfig ),
                                                   databaseConfig, LeaseService.NO_LEASES, editionDatabaseComponents.getExternalIdReuseConditionProvider(),
                                                   storageEngineFactory, edition.getReadOnlyChecker(), tracers );
    }

    private void forEachDatabase( BiConsumer<NamedDatabaseId,DB> consumer, boolean systemDatabaseLast, String operationName )
    {
        var snapshot = systemDatabaseLast ? databasesSnapshot().descendingMap().entrySet() : databasesSnapshot().entrySet();
        DatabaseManagementException dbmsExceptions = null;

        for ( var entry : snapshot )
        {
            NamedDatabaseId namedDatabaseId = entry.getKey();
            DB context = entry.getValue();
            try
            {
                consumer.accept( namedDatabaseId, context );
            }
            catch ( Throwable t )
            {
                var dbmsException = new DatabaseManagementException( format( "An error occurred! Unable to %s the database `%s`.",
                        operationName, namedDatabaseId ), t );
                dbmsExceptions = Exceptions.chain( dbmsExceptions, dbmsException );
            }
        }

        if ( dbmsExceptions != null )
        {
            throw dbmsExceptions;
        }
    }

    protected void startDatabase( NamedDatabaseId namedDatabaseId, DB context )
    {
        try
        {
            log.info( "Starting '%s'.", namedDatabaseId );
            Database database = context.database();
            database.start();
        }
        catch ( Throwable t )
        {
            throw new DatabaseManagementException( format( "An error occurred! Unable to start `%s`.", namedDatabaseId ), t );
        }
    }

    protected void stopDatabase( NamedDatabaseId namedDatabaseId, DB context )
    {
        try
        {
            log.info( "Stopping '%s'.", namedDatabaseId );
            Database database = context.database();
            database.stop();
            log.info( "Stopped '%s' successfully.", namedDatabaseId );
        }
        catch ( Throwable t )
        {
            log.error( "Error stopping '%s'.", namedDatabaseId );
            throw new DatabaseManagementException( format( "An error occurred! Unable to stop `%s`.", namedDatabaseId ), t );
        }
    }

    protected final VersionContextSupplier createVersionContextSupplier( DatabaseConfig databaseConfig )
    {
        var factory = externalVersionContextSupplierFactory( globalModule )
                .orElse( internalVersionContextSupplierFactory( databaseConfig ) );
        return factory.create();
    }

    private static Optional<VersionContextSupplier.Factory> externalVersionContextSupplierFactory( GlobalModule globalModule )
    {
        var externalDependencies = globalModule.getExternalDependencyResolver();
        var klass = VersionContextSupplier.Factory.class;
        if ( externalDependencies.containsDependency( klass ) )
        {
            return Optional.of( externalDependencies.resolveDependency( klass ) );
        }
        return Optional.empty();
    }

    private static VersionContextSupplier.Factory internalVersionContextSupplierFactory( DatabaseConfig databaseConfig )
    {
        return () -> databaseConfig.get( snapshot_query ) ? new TransactionVersionContextSupplier() : EmptyVersionContextSupplier.EMPTY;
    }
}
