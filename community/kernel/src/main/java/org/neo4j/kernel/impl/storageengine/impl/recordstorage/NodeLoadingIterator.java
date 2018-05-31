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
package org.neo4j.kernel.impl.storageengine.impl.recordstorage;

import org.eclipse.collections.api.iterator.LongIterator;

import org.neo4j.cursor.Cursor;
import org.neo4j.function.ThrowingLongFunction;
import org.neo4j.helpers.collection.PrefetchingIterator;
import org.neo4j.internal.kernel.api.exceptions.EntityNotFoundException;
import org.neo4j.storageengine.api.NodeItem;

class NodeLoadingIterator extends PrefetchingIterator<Cursor<NodeItem>>
{
    private final LongIterator ids;
    private final ThrowingLongFunction<Cursor<NodeItem>,EntityNotFoundException> loader;

    NodeLoadingIterator( LongIterator ids, ThrowingLongFunction<Cursor<NodeItem>,EntityNotFoundException> loader )
    {
        this.ids = ids;
        this.loader = loader;
    }

    @Override
    protected Cursor<NodeItem> fetchNextOrNull()
    {
        while ( ids.hasNext() )
        {
            try
            {
                return loader.apply( ids.next() );
            }
            catch ( EntityNotFoundException e )
            {
                // skip this id
            }
        }

        return null;
    }
}
