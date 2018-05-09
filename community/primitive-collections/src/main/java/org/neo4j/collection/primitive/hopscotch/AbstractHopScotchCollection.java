/*
 * Copyright (c) 2002-2018 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
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
package org.neo4j.collection.primitive.hopscotch;

import org.neo4j.collection.primitive.PrimitiveCollection;
import org.neo4j.collection.primitive.hopscotch.HopScotchHashingAlgorithm.ResizeMonitor;

/**
 * Typical design of a hop scotch collection holding a table and communicating with
 * {@link HopScotchHashingAlgorithm} It's a {@link ResizeMonitor} which will have the {@link Table}
 * reassigned when it grows.
 */
public abstract class AbstractHopScotchCollection<VALUE> implements PrimitiveCollection, ResizeMonitor<VALUE>
{
    protected Table<VALUE> table;

    public AbstractHopScotchCollection( Table<VALUE> table )
    {
        this.table = table;
    }

    @Override
    public int size()
    {
        return table.size();
    }

    @Override
    public boolean isEmpty()
    {
        return table.isEmpty();
    }

    @Override
    public String toString()
    {
        return table.toString();
    }

    @Override
    public void clear()
    {
        table.clear();
    }

    @Override
    public void tableGrew( Table<VALUE> newTable )
    {
        this.table = newTable;
    }

    @Override
    public Table<VALUE> getLastTable()
    {
        return table;
    }

    @Override
    public void close()
    {
        table.close();
    }

    @Override
    public abstract boolean equals( Object other );

    @Override
    public abstract int hashCode();

    protected final boolean typeAndSizeEqual( Object other )
    {
        if ( this.getClass() == other.getClass() )
        {
            AbstractHopScotchCollection that = (AbstractHopScotchCollection) other;
            if ( this.size() == that.size() )
            {
                return true;
            }
        }
        return false;
    }
}
