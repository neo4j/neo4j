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
package org.neo4j.bolt.runtime;

import org.junit.jupiter.api.Test;

import org.neo4j.bolt.v4.messaging.MessageMetadataParser;
import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.kernel.database.DatabaseId;
import org.neo4j.kernel.database.DatabaseIdRepository;
import org.neo4j.kernel.database.PlaceholderDatabaseIdRepository;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

class BoltDatabaseIdRepositoryTest
{
    private String defaultDbName = "defaultDatabaseName";
    private Config config = Config.defaults( GraphDatabaseSettings.default_database, defaultDbName );
    private DatabaseIdRepository delegate = new PlaceholderDatabaseIdRepository( config );
    private DatabaseIdRepository databaseIdRepository = new BoltDatabaseIdRepository( delegate );

    @Test
    void shouldReturnDefaultDatabaseIfGetAbsentDBName()
    {
        // when
        var databaseId = databaseIdRepository.get( MessageMetadataParser.ABSENT_DB_NAME );

        // then
        assertThat( databaseId, equalTo( databaseIdRepository.defaultDatabase() ) );
    }

    @Test
    void shouldDelegateGet()
    {
        // when
        String databaseName = "db";
        DatabaseId databaseId = databaseIdRepository.get( databaseName );

        // then
        assertThat( databaseId, equalTo( delegate.get( databaseName ) ) );
    }

    @Test
    void shouldDelegateDefaultDatabase()
    {
        // when
        DatabaseId databaseId = databaseIdRepository.defaultDatabase();

        // then
        assertThat( databaseId, equalTo( delegate.defaultDatabase() ) );
    }

    @Test
    void shouldDelegateSystemDatabase()
    {
        // when
        DatabaseId databaseId = databaseIdRepository.systemDatabase();

        // then
        assertThat( databaseId, equalTo( delegate.systemDatabase() ) );
    }
}
