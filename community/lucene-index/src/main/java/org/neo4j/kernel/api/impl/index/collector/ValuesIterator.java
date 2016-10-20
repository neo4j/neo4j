/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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
package org.neo4j.kernel.api.impl.index.collector;

import org.neo4j.collection.primitive.PrimitiveLongCollections;

/**
 * Document values iterators that are primitive long iterators that can access value by field from document
 * and provides information about how many items remains in the underlying source.
 */
public abstract class ValuesIterator extends PrimitiveLongCollections.PrimitiveLongBaseIterator
        implements DocValuesAccess
{
    public static final ValuesIterator EMPTY = new ValuesIterator( 0 )
    {
        @Override
        public int remaining()
        {
            return 0;
        }

        @Override
        protected boolean fetchNext()
        {
            return false;
        }

        @Override
        public long current()
        {
            return 0;
        }

        @Override
        public long getValue( String field )
        {
            return 0;
        }
    };

    protected final int size;
    protected int index = 0;

    ValuesIterator( int size )
    {
        this.size = size;
    }

    /**
     * @return the number of docs left in this iterator.
     */
    public int remaining()
    {
        return size - index;
    }
}
