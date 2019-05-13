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

import org.junit.jupiter.api.Test;

import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseSettings;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.neo4j.configuration.GraphDatabaseSettings.SYSTEM_DATABASE_NAME;

class PlaceholderDatabaseIdRepositoryTest
{
    private String defaultDBName = "default database name";
    private DatabaseIdRepository databaseIdRepository =
            new PlaceholderDatabaseIdRepository( Config.defaults( GraphDatabaseSettings.default_database, defaultDBName ) );

    @Test
    void shouldReturnDefaultDatabase()
    {
        DatabaseId databaseId = databaseIdRepository.defaultDatabase();

        assertThat( databaseId.name(), equalTo( defaultDBName ) );
    }

    @Test
    void shouldReturnSystemDatabase()
    {
        DatabaseId databaseId = databaseIdRepository.systemDatabase();

        assertThat( databaseId.name(), equalTo( SYSTEM_DATABASE_NAME ) );
    }

    @Test
    void shouldCreateNewDatabaseId()
    {
        var dbName = "foo-bar-baz";
        DatabaseId databaseId = databaseIdRepository.get( dbName );

        assertThat( databaseId.name(), equalTo( dbName ) );
    }
}
