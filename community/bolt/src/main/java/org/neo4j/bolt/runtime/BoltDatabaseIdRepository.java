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

import org.neo4j.kernel.database.DatabaseId;
import org.neo4j.kernel.database.DatabaseIdRepository;

import static org.neo4j.bolt.v4.messaging.MessageMetadataParser.ABSENT_DB_NAME;

public class BoltDatabaseIdRepository implements DatabaseIdRepository
{
    private final DatabaseIdRepository delegate;

    public BoltDatabaseIdRepository( DatabaseIdRepository delegate )
    {
        this.delegate = delegate;
    }

    @Override
    public DatabaseId get( String databaseName )
    {
        if ( ABSENT_DB_NAME.equals( databaseName ) )
        {
            return defaultDatabase();
        }
        else
        {
            return delegate.get( databaseName );
        }
    }

    @Override
    public DatabaseId defaultDatabase()
    {
        return delegate.defaultDatabase();
    }

    @Override
    public DatabaseId systemDatabase()
    {
        return delegate.systemDatabase();
    }
}
