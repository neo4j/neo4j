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
import java.util.UUID;

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
        this.defaultDatabaseName = defaultDbName;
    }

    public DatabaseId defaultDatabase()
    {
        return getRaw( defaultDatabaseName );
    }

    public DatabaseId getRaw( String databaseName )
    {
        var databaseIdOpt = get( databaseName );
        Preconditions.checkState( databaseIdOpt.isPresent(),
                getClass().getSimpleName() + " should always produce a " + DatabaseId.class.getSimpleName() + " for any database name" );
        return databaseIdOpt.get();
    }

    /**
     * @return a DatabaseId with a random name and UUID. It is not stored, so subsequent calls will return different DatabaseIds.
     */
    public static DatabaseId randomDatabaseId()
    {
        return new DatabaseId( RandomStringUtils.randomAlphabetic( 20 ), UUID.randomUUID() );
    }

    private static class RandomDatabaseIdRepository implements DatabaseIdRepository
    {
        @Override
        public Optional<DatabaseId> get( NormalizedDatabaseName databaseName )
        {
            return Optional.of( new DatabaseId( databaseName.name(), UUID.randomUUID() ) );
        }
    }

    /**
     * Disable SystemGraphInitializer to avoid interfering with tests or changing backups. Injects {@link TestDatabaseIdRepository} as it will no longer
     * be possible to read {@link DatabaseId} from system database.
     * Assumes default database name is {@link GraphDatabaseSettings#DEFAULT_DATABASE_NAME}
     * @return Dependencies that can set as external dependencies in DatabaseManagementServiceBuilder
     */
    public static Dependencies noOpSystemGraphInitializer()
    {
        return noOpSystemGraphInitializer( Config.defaults() );
    }

    /**
     * Disable SystemGraphInitializer to avoid interfering with tests or changing backups. Injects {@link TestDatabaseIdRepository} as it will no longer
     * be possible to read {@link DatabaseId} from system database.
     * @param config Used for default database name
     * @return Dependencies that can set as external dependencies in DatabaseManagementServiceBuilder
     */
    public static Dependencies noOpSystemGraphInitializer( Config config )
    {
        return noOpSystemGraphInitializer( new Dependencies(), config );
    }

    /**
     * Disable SystemGraphInitializer to avoid interfering with tests or changing backups. Injects {@link TestDatabaseIdRepository} as it will no longer
     * be possible to read {@link DatabaseId} from system database.
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
