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
package org.neo4j.kernel.api.impl.index.collector;

import org.neo4j.collection.primitive.PrimitiveLongCollections;
import org.neo4j.collection.primitive.PrimitiveLongIterator;

/**
 * Document values iterators that are primitive long iterators that can access value by field from document
 * and provides information about how many items remains in the underlying source.
 */
public interface ValuesIterator extends PrimitiveLongIterator, DocValuesAccess
{
    int remaining();

    float currentScore();

    ValuesIterator EMPTY = new ValuesIterator.Adapter( 0 )
    {
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
        public float currentScore()
        {
            return 0;
        }

        @Override
        public long getValue( String field )
        {
            return 0;
        }
    };

    abstract class Adapter extends PrimitiveLongCollections.PrimitiveLongBaseIterator implements ValuesIterator
    {
        protected final int size;
        protected int index;

        /**
         * Gets the score for the current iterator position.
         *
         * @return The score of the value, or 0 if scoring is not kept or applicable.
         */
        public abstract float currentScore();

        Adapter( int size )
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
}
