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

import org.apache.commons.lang3.mutable.MutableLong;

import java.util.Iterator;
import java.util.Map;
import org.neo4j.helpers.collection.Iterators;

/**
 * Keeps data about how relationships are distributed between different types.
 */
public class RelationshipTypeDistribution implements Iterable<Map.Entry<Object,MutableLong>>
{
    private final Map.Entry<Object,MutableLong>[] sortedTypes;

    public RelationshipTypeDistribution( Map.Entry<Object,MutableLong>[] sortedTypes )
    {
        this.sortedTypes = sortedTypes;
    }

    @Override
    public Iterator<Map.Entry<Object,MutableLong>> iterator()
    {
        return Iterators.iterator( sortedTypes );
    }

    public int getNumberOfRelationshipTypes()
    {
        return sortedTypes.length;
    }
}
