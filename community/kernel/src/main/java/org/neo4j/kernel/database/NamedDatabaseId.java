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

import org.neo4j.configuration.helpers.NormalizedDatabaseName;

import static java.util.Objects.requireNonNull;

/**
 * Represents a unique identifier for a database, including a persistent and immutable component. Intended to support renaming of database.
 * <p>
 * Cannot be used to represent a database that has not been created yet or has not been resolved from persistent storage yet.
 * <p>
 * Create using a {@link DatabaseIdRepository}, or if reading from persistence or network use a {@link DatabaseIdFactory}
 * <p>
 * Equality and hashcode are based only on UUID.
 */
public class NamedDatabaseId implements Comparable<NamedDatabaseId>
{
    private final String name;
    private final DatabaseId databaseId;

    NamedDatabaseId( String name, UUID uuid )
    {
        this( name, new DatabaseId( uuid ) );
    }

    NamedDatabaseId( String name, DatabaseId databaseId )
    {
        requireNonNull( databaseId, "DatabaseId should be not null." );
        requireNonNull( name, "Database name should be not null." );
        this.databaseId = databaseId;
        this.name = new NormalizedDatabaseName( name ).name();
    }

    public String name()
    {
        return name;
    }

    public DatabaseId databaseId()
    {
        return databaseId;
    }

    public String logPrefix()
    {
        return name + "/" + databaseId.id();
    }

    @Override
    public String toString()
    {
        return "DatabaseId{" + databaseId.id() + "[" + name + "]}";
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
        NamedDatabaseId that = (NamedDatabaseId) o;
        return databaseId.equals( that.databaseId );
    }

    @Override
    public int hashCode()
    {
        return Objects.hash( databaseId );
    }

    @Override
    public int compareTo( NamedDatabaseId that )
    {
        boolean leftIsSystem = this.isSystemDatabase();
        boolean rightIsSystem = that.isSystemDatabase();
        if ( leftIsSystem || rightIsSystem )
        {
            return Boolean.compare( rightIsSystem, leftIsSystem );
        }
        else
        {
            return this.name.compareTo( that.name );
        }
    }

    public boolean isSystemDatabase()
    {
        return databaseId.isSystemDatabase();
    }
}
