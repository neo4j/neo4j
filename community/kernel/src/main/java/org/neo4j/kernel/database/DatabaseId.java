/*
 * Copyright (c) 2002-2020 "Neo4j,"
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

import java.util.Objects;
import java.util.UUID;

import org.neo4j.util.Id;

public class DatabaseId
{
    private final Id id;

    DatabaseId( UUID uuid )
    {
        Objects.requireNonNull( uuid, "Database UUID should be not null." );
        id = new Id( uuid );
    }

    public UUID uuid()
    {
        return id.uuid();
    }

    Id id()
    {
        return id;
    }

    public boolean isSystemDatabase()
    {
        return DatabaseIdRepository.NAMED_SYSTEM_DATABASE_ID.databaseId().equals( this );
    }

    @Override
    public boolean equals( Object o )
    {
        if ( this == o )
        {
            return true;
        }
        if ( o == null || getClass() != o.getClass() )
        {
            return false;
        }
        DatabaseId that = (DatabaseId) o;
        return Objects.equals( id, that.id );
    }

    @Override
    public int hashCode()
    {
        return Objects.hash( id );
    }

    @Override
    public String toString()
    {
        return "DatabaseId{" + id + '}';
    }
}
