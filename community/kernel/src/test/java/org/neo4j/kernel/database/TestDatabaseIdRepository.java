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

import org.apache.commons.lang3.RandomStringUtils;

import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArraySet;

import org.neo4j.collection.Dependencies;
import org.neo4j.common.DependencyResolver;
import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.configuration.helpers.NormalizedDatabaseName;
import org.neo4j.dbms.database.SystemGraphInitializer;
import org.neo4j.util.Preconditions;

import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;

public class TestDatabaseIdRepository extends MapCachingDatabaseIdRepository
{
    private final String defaultDatabaseName;
    private final Set<NamedDatabaseId> filterSet;

    public TestDatabaseIdRepository()
    {
        this( DEFAULT_DATABASE_NAME );
    }

    public TestDatabaseIdRepository( Config config )
    {
        this( config.get( GraphDatabaseSettings.default_database ) );
    }

    public TestDatabaseIdRepository( String defaultDbName )
    {
        super( new RandomDatabaseIdRepository() );
        filterSet = new CopyOnWriteArraySet<>();
        this.defaultDatabaseName = defaultDbName;
    }

    public NamedDatabaseId defaultDatabase()
    {
        return getRaw( defaultDatabaseName );
    }

    public NamedDatabaseId getRaw( String databaseName )
    {
        var databaseIdOpt = getByName( databaseName );
        Preconditions.checkState( databaseIdOpt.isPresent(),
                getClass().getSimpleName() + " should always produce a " + NamedDatabaseId.class.getSimpleName() + " for any database name" );
        var databaseId = databaseIdOpt.get();
        cache( databaseId );
        return databaseId;
    }

    /**
     * Add a database to appear "not found" by the id repository
     */
    public void filter( NamedDatabaseId namedDatabaseId )
    {
        filterSet.add( namedDatabaseId );
    }

    /**
     * Remove a database from the filter set, allowing it to be found by the "always found" default implementation of TestDatabaseIdRepository
     */
    public void unfilter( NamedDatabaseId namedDatabaseId )
    {
        filterSet.remove( namedDatabaseId );
    }

    @Override
    public Optional<NamedDatabaseId> getByName( NormalizedDatabaseName databaseName )
    {
        var id = super.getByName( databaseName );
        var nameIsFiltered = id.map( filterSet::contains ).orElse( false );
        return nameIsFiltered ? Optional.empty() : id;
    }

    @Override
    public Optional<NamedDatabaseId> getById( DatabaseId databaseId )
    {
        var id = super.getById( databaseId );
        var uuidIsFiltered = id.map( filterSet::contains ).orElse( false );
        return uuidIsFiltered ? Optional.empty() : id;
    }

    /**
     * @return a DatabaseId with a random name and UUID. It is not stored, so subsequent calls will return different DatabaseIds.
     */
    public static NamedDatabaseId randomNamedDatabaseId()
    {
        return new NamedDatabaseId( RandomStringUtils.randomAlphabetic( 20 ), UUID.randomUUID() );
    }

    public static DatabaseId randomDatabaseId()
    {
        return new DatabaseId( UUID.randomUUID() );
    }

    private static class RandomDatabaseIdRepository implements DatabaseIdRepository
    {
        @Override
        public Optional<NamedDatabaseId> getByName( NormalizedDatabaseName databaseName )
        {
            return Optional.of( new NamedDatabaseId( databaseName.name(), UUID.randomUUID() ) );
        }

        @Override
        public Optional<NamedDatabaseId> getById( DatabaseId databaseId )
        {
            return Optional.of( new NamedDatabaseId( "db" + databaseId.hashCode(), databaseId.uuid() ) );
        }
    }

    /**
     * Disable SystemGraphInitializer to avoid interfering with tests or changing backups. Injects {@link TestDatabaseIdRepository} as it will no longer
     * be possible to read {@link NamedDatabaseId} from system database.
     * Assumes default database name is {@link GraphDatabaseSettings#DEFAULT_DATABASE_NAME}
     * @return Dependencies that can set as external dependencies in DatabaseManagementServiceBuilder
     */
    public static Dependencies noOpSystemGraphInitializer()
    {
        return noOpSystemGraphInitializer( Config.defaults() );
    }

    /**
     * Disable SystemGraphInitializer to avoid interfering with tests or changing backups. Injects {@link TestDatabaseIdRepository} as it will no longer
     * be possible to read {@link NamedDatabaseId} from system database.
     * @param config Used for default database name
     * @return Dependencies that can set as external dependencies in DatabaseManagementServiceBuilder
     */
    public static Dependencies noOpSystemGraphInitializer( Config config )
    {
        return noOpSystemGraphInitializer( new Dependencies(), config );
    }

    /**
     * Disable SystemGraphInitializer to avoid interfering with tests or changing backups. Injects {@link TestDatabaseIdRepository} as it will no longer
     * be possible to read {@link NamedDatabaseId} from system database.
     * @param dependencies to include in returned {@link DependencyResolver}
     * @param config Used for default database name
     * @return Dependencies that can set as external dependencies in DatabaseManagementServiceBuilder
     */
    public static Dependencies noOpSystemGraphInitializer( DependencyResolver dependencies, Config config )
    {
        return noOpSystemGraphInitializer( new Dependencies( dependencies ), config );
    }

    private static Dependencies noOpSystemGraphInitializer( Dependencies dependencies, Config config )
    {
        dependencies.satisfyDependencies( SystemGraphInitializer.NO_OP, new TestDatabaseIdRepository( config ) );
        return dependencies;
    }
}
