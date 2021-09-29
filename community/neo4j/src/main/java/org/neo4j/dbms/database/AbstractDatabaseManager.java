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

import org.neo4j.common.DependencyResolver;
import org.neo4j.configuration.Config;
import org.neo4j.dbms.api.DatabaseManagementException;
import org.neo4j.graphdb.DatabaseShutdownException;
import org.neo4j.graphdb.factory.module.GlobalModule;
import org.neo4j.graphdb.factory.module.edition.CommunityEditionModule;
import org.neo4j.internal.helpers.Exceptions;
import org.neo4j.kernel.database.Database;
import org.neo4j.kernel.database.DatabaseId;
import org.neo4j.kernel.database.DatabaseIdRepository;
import org.neo4j.kernel.database.MapCachingDatabaseIdRepository;
import org.neo4j.kernel.database.NamedDatabaseId;
import org.neo4j.kernel.database.SystemGraphDatabaseIdRepository;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;
import org.neo4j.logging.Log;

import static java.lang.String.format;
import static java.util.Collections.unmodifiableNavigableMap;

public abstract class AbstractDatabaseManager<DB extends DatabaseContext> extends LifecycleAdapter implements DatabaseManager<DB>
{
    protected final Map<NamedDatabaseId,DB> databaseMap;
    protected final DependencyResolver externalDependencyResolver;
    protected final Log log;
    protected final boolean manageDatabasesOnStartAndStop;
    protected final Config config;
    protected final DatabaseContextFactory<DB> databaseContextFactory;

    private final DatabaseIdRepository.Caching databaseIdRepository;

    protected AbstractDatabaseManager( GlobalModule globalModule, DatabaseContextFactory<DB> databaseContextFactory, boolean manageDatabasesOnStartAndStop )
    {
        this.log = globalModule.getLogService().getInternalLogProvider().getLog( getClass() );
        this.externalDependencyResolver = globalModule.getExternalDependencyResolver();
        this.config = globalModule.getGlobalConfig();
        this.manageDatabasesOnStartAndStop = manageDatabasesOnStartAndStop;
        this.databaseMap = new ConcurrentHashMap<>();
        this.databaseContextFactory = databaseContextFactory;
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

    private DB getSystemDatabaseContext()
    {
        return getDatabaseContext( NamedDatabaseId.NAMED_SYSTEM_DATABASE_ID )
                .orElseThrow( () -> new DatabaseShutdownException( new DatabaseManagementException( "Unable to retrieve the system database!" ) ) );
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
}
