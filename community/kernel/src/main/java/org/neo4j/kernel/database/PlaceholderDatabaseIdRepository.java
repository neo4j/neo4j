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

import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseSettings;

import static org.neo4j.configuration.GraphDatabaseSettings.SYSTEM_DATABASE_NAME;

/**
 * Temporary implementation of {@link DatabaseIdRepository}. Does not persist {@link DatabaseId}, exists as an intermediary step in the use of
 * DatabaseIdRepository.
 *
 * To be replaced with a persisting implementation. For further details see https://trello.com/c/3ajorJyU
 */
public class PlaceholderDatabaseIdRepository implements DatabaseIdRepository
{
    private final DatabaseId defaultDatabase;
    private final DatabaseId systemDatabase;

    public PlaceholderDatabaseIdRepository( Config config )
    {
        defaultDatabase = get( config.get( GraphDatabaseSettings.default_database ) );
        systemDatabase = get( SYSTEM_DATABASE_NAME );
    }

    @Override
    public DatabaseId get( String databaseName )
    {
        return new DatabaseId( databaseName );
    }

    @Override
    public DatabaseId defaultDatabase()
    {
        return defaultDatabase;
    }

    @Override
    public DatabaseId systemDatabase()
    {
        return systemDatabase;
    }
}
