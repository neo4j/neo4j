/*
 * Copyright (c) 2002-2018 "Neo4j,"
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
package org.neo4j.kernel.impl.util.collection;

import org.eclipse.collections.api.iterator.LongIterator;
import org.eclipse.collections.api.set.primitive.LongSet;
import org.eclipse.collections.impl.factory.primitive.LongSets;
import org.eclipse.collections.impl.iterator.ImmutableEmptyLongIterator;

public interface RichLongSet extends LongSet
{
    LongIterator rangeIterator( int start, int stop );

    @Override
    RichLongSet freeze();

    RichLongSet EMPTY = new EmptyRichLongSet();

    final class EmptyRichLongSet extends BaseRichLongSet
    {
        private EmptyRichLongSet()
        {
            super( LongSets.immutable.empty() );
        }

        @Override
        public LongIterator rangeIterator( int start, int stop )
        {
            return ImmutableEmptyLongIterator.INSTANCE;
        }

        @Override
        public RichLongSet freeze()
        {
            return this;
        }
    }
}
