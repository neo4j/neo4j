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

import org.eclipse.collections.impl.block.factory.Comparators;

import java.util.Comparator;
import java.util.Objects;

import org.neo4j.kernel.database.DatabaseId;

import static org.neo4j.configuration.GraphDatabaseSettings.SYSTEM_DATABASE_NAME;

/**
 * This is a custom comparator for databases, which always places the system database to be the first (lowest) in any sorted order.
 * A custom ordering may be provided for the rest of the managed databases, in the form of a wrapped "delegate" comparator. However,
 * regardless of what comparator is provided, system database will always sort lower than every other database.
 *
 * If no custom comparator is provided then the databases are sorted lexicographically by their name.
 */
public class DatabasesComparator implements Comparator<DatabaseId>
{
    private final Comparator<DatabaseId> baseComparator;

    public DatabasesComparator()
    {
        this.baseComparator = Comparators.naturalOrder();
    }

    @Override
    public int compare( DatabaseId left, DatabaseId right )
    {
        boolean leftIsSystem = isSystemDatabase( left );
        boolean rightIsSystem = isSystemDatabase( right );
        if ( leftIsSystem || rightIsSystem )
        {
            return Boolean.compare( rightIsSystem, leftIsSystem );
        }
        else
        {
            return baseComparator.compare( left, right );
        }
    }

    private boolean isSystemDatabase( DatabaseId id )
    {
        return Objects.equals( id.name(), SYSTEM_DATABASE_NAME );
    }
}
