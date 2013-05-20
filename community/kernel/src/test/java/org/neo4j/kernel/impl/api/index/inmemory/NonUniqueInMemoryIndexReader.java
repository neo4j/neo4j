/**
 * Copyright (c) 2002-2013 "Neo Technology,"
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
package org.neo4j.kernel.impl.api.index.inmemory;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import org.neo4j.helpers.collection.IteratorUtil;
import org.neo4j.kernel.api.index.IndexReader;

class NonUniqueInMemoryIndexReader implements IndexReader
{
    private final HashMap<Object, Set<Long>> indexData;

    NonUniqueInMemoryIndexReader( Map<Object, Set<Long>> indexData )
    {
        this.indexData = new HashMap<Object, Set<Long>>( indexData );
    }

    @Override
    public Iterator<Long> lookup( Object value )
    {
        Set<Long> result = indexData.get( value );
        return result != null ? result.iterator() : IteratorUtil.<Long>emptyIterator();
    }

    @Override
    public void close()
    {
    }
}
