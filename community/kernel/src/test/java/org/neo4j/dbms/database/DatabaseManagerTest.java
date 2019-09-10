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

import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.Optional;
import java.util.SortedMap;

import org.neo4j.configuration.helpers.NormalizedDatabaseName;
import org.neo4j.dbms.api.DatabaseExistsException;
import org.neo4j.dbms.api.DatabaseNotFoundException;
import org.neo4j.kernel.database.DatabaseId;
import org.neo4j.kernel.database.DatabaseIdRepository;
import org.neo4j.kernel.database.NamedDatabaseId;
import org.neo4j.kernel.database.TestDatabaseIdRepository;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class DatabaseManagerTest
{
    private static final String KNOWN_DATABASE_NAME = "known";
    private static final String UNKNOWN_DATABASE_NAME = "unknown";
    private static final NamedDatabaseId DATABASE_ID_NAMED = TestDatabaseIdRepository.randomNamedDatabaseId();

    private DatabaseManager<?> databaseManager = new TestDatabaseManager();

    @Test
    void shouldReturnContextForKnownDatabaseName()
    {
        assertTrue( databaseManager.getDatabaseContext( KNOWN_DATABASE_NAME ).isPresent() );
    }

    @Test
    void shouldReturnEmptyForUnknownDatabaseName()
    {
        assertFalse( databaseManager.getDatabaseContext( UNKNOWN_DATABASE_NAME ).isPresent() );
    }

    private static class TestDatabaseManager implements DatabaseManager<DatabaseContext>
    {
        @Override
        public Optional<DatabaseContext> getDatabaseContext( NamedDatabaseId namedDatabaseId )
        {
            return Optional.of( Mockito.mock( DatabaseContext.class ) );
        }

        @Override
        public DatabaseIdRepository.Caching databaseIdRepository()
        {
            return new DatabaseIdRepository.Caching()
            {
                @Override
                public void invalidate( NamedDatabaseId namedDatabaseId )
                {

                }

                @Override
                public void cache( NamedDatabaseId namedDatabaseId )
                {

                }

                @Override
                public Optional<NamedDatabaseId> getByName( NormalizedDatabaseName databaseName )
                {
                    if ( KNOWN_DATABASE_NAME.equals( databaseName.name() ) )
                    {
                        return Optional.of( DATABASE_ID_NAMED );
                    }
                    else
                    {
                        return Optional.empty();
                    }
                }

                @Override
                public Optional<NamedDatabaseId> getById( DatabaseId databaseId )
                {
                    if ( DATABASE_ID_NAMED.databaseId().equals( databaseId ) )
                    {
                        return Optional.of( DATABASE_ID_NAMED );
                    }
                    else
                    {
                        return Optional.empty();
                    }
                }
            };
        }

        @Override
        public DatabaseContext createDatabase( NamedDatabaseId namedDatabaseId ) throws DatabaseExistsException
        {
            return null;
        }

        @Override
        public void dropDatabase( NamedDatabaseId namedDatabaseId ) throws DatabaseNotFoundException
        {

        }

        @Override
        public void stopDatabase( NamedDatabaseId namedDatabaseId ) throws DatabaseNotFoundException
        {

        }

        @Override
        public void startDatabase( NamedDatabaseId namedDatabaseId ) throws DatabaseNotFoundException
        {

        }

        @Override
        public SortedMap<NamedDatabaseId,DatabaseContext> registeredDatabases()
        {
            return null;
        }

        @Override
        public void init() throws Exception
        {

        }

        @Override
        public void start() throws Exception
        {

        }

        @Override
        public void stop() throws Exception
        {

        }

        @Override
        public void shutdown() throws Exception
        {

        }
    }
}
