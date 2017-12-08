/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
package org.neo4j.unsafe.impl.batchimport;

import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import org.neo4j.helpers.collection.Iterators;
import org.neo4j.helpers.collection.Pair;

import static java.lang.String.format;

/**
 * Keeps data about how relationships are distributed between different types.
 */
public class DataStatistics implements Iterable<Pair<Object,Long>>
{
    // keys can be either String or Integer
    private final Pair<Object,Long>[] sortedTypes;
    private final long nodeCount;
    private final long propertyCount;

    public DataStatistics( long nodeCount, long propertyCount, Pair<Object,Long>[] sortedTypes )
    {
        this.nodeCount = nodeCount;
        this.propertyCount = propertyCount;
        this.sortedTypes = sortedTypes;
    }

    @Override
    public Iterator<Pair<Object,Long>> iterator()
    {
        return Iterators.iterator( sortedTypes );
    }

    public int getNumberOfRelationshipTypes()
    {
        return sortedTypes.length;
    }

    public Pair<Object,Long> get( int index )
    {
        return sortedTypes[index];
    }

    public Set<Object> types( int startingFromType, int upToType )
    {
        Set<Object> types = new HashSet<>();
        for ( int i = startingFromType; i < upToType; i++ )
        {
            types.add( get( i ).first() );
        }
        return types;
    }

    public long getNodeCount()
    {
        return nodeCount;
    }

    public long getPropertyCount()
    {
        return propertyCount;
    }

    public long getRelationshipCount()
    {
        long sum = 0;
        for ( Pair<Object,Long> type : sortedTypes )
        {
            sum += type.other();
        }
        return sum;
    }

    @Override
    public String toString()
    {
        return format( "Imported:%n  %d nodes%n  %d relationships%n  %d properties", nodeCount, getRelationshipCount(), propertyCount );
    }
}
