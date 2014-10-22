/**
 * Copyright (c) 2002-2014 "Neo Technology,"
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
package org.neo4j.kernel.api.index;

import org.neo4j.collection.primitive.PrimitiveLongCollections;
import org.neo4j.collection.primitive.PrimitiveLongIterator;
import org.neo4j.graphdb.Resource;

/**
 * Reader for an {@link IndexAccessor}.
 * Must honor repeatable reads, which means that if a lookup is executed multiple times the same result set
 * must be returned.
 */
public interface IndexReader extends Resource
{
    PrimitiveLongIterator lookup( Object value );

    IndexReader EMPTY = new IndexReader()
    {
        @Override
        public PrimitiveLongIterator lookup( Object value )
        {
            return PrimitiveLongCollections.emptyIterator();
        }

        // Used for checking index correctness
        @Override
        public int getIndexedCount( long nodeId, Object propertyValue )
        {
            return 0;
        }

        @Override
        public void sampleIndex( ValueSampler sampler )
        {
        }

        @Override
        public void close()
        {
        }
    };

    /**
     * Number of nodes indexed by the given property
     */
    int getIndexedCount( long nodeId, Object propertyValue );

    /**
     * Sample this index (on the current thread)
     * @param sampler to use for reporting values
     *
     */
    public void sampleIndex( ValueSampler sampler );
}
