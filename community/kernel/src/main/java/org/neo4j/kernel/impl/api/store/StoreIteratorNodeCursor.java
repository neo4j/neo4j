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
package org.neo4j.kernel.impl.api.store;

import org.neo4j.collection.primitive.PrimitiveLongIterator;
import org.neo4j.function.Consumer;
import org.neo4j.graphdb.Resource;
import org.neo4j.kernel.impl.locking.LockService;
import org.neo4j.kernel.impl.store.NeoStores;
import org.neo4j.kernel.impl.store.record.NodeRecord;

/**
 * Cursor for iterating a set of nodes. It is attached to an iterator, typically from
 * an index seek or similar.
 */
public class StoreIteratorNodeCursor extends StoreAbstractNodeCursor
{
    private PrimitiveLongIterator iterator;
    private final Consumer<StoreIteratorNodeCursor> instanceCache;

    public StoreIteratorNodeCursor( NodeRecord nodeRecord,
            NeoStores neoStores,
            StoreStatement storeStatement,
            Consumer<StoreIteratorNodeCursor> instanceCache,
            LockService lockService )
    {
        super( nodeRecord, neoStores, storeStatement, lockService );
        this.instanceCache = instanceCache;
    }

    public StoreIteratorNodeCursor init( PrimitiveLongIterator iterator )
    {
        this.iterator = iterator;
        return this;
    }

    @Override
    public boolean next()
    {
        while ( iterator != null && iterator.hasNext() )
        {
            NodeRecord record = nodeStore.loadRecord( iterator.next(), nodeRecord );
            if ( record != null && record.inUse() )
            {
                return true;
            }
        }

        return false;
    }

    @Override
    public void close()
    {
        if ( iterator instanceof Resource )
        {
            ((Resource) iterator).close();
        }
        iterator = null;

        instanceCache.accept( this );
    }
}
