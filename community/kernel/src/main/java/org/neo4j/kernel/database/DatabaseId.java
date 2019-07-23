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

import java.util.Objects;

import static java.util.Objects.requireNonNull;
import static org.neo4j.configuration.GraphDatabaseSettings.SYSTEM_DATABASE_NAME;

/**
 * Represents a unique identifier for a database, including a persistent and immutable component. Intended to support renaming of database.
 *
 * Cannot be used to represent a database that has not been created yet or has not been resolved from persistent storage yet.
 *
 * TODO: a persistent and immutable component. For further details see https://trello.com/c/3ajorJyU
 */
public class DatabaseId implements Comparable<DatabaseId>
{
    private final String name;

    public DatabaseId( String name )
    {
        requireNonNull( name, "Database name should be not null." );
        this.name = name.toLowerCase();
    }

    public String name()
    {
        return name;
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
        return Objects.equals( name, that.name );
    }

    @Override
    public int hashCode()
    {
        return Objects.hash( name );
    }

    @Override
    public String toString()
    {
        return "DatabaseId{" + "name='" + name + '\'' + '}';
    }

    @Override
    public int compareTo( DatabaseId that )
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
        return SYSTEM_DATABASE_NAME.equals( name() );
    }
}
