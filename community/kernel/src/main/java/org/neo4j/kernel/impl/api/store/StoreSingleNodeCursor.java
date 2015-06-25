/*
 * Copyright (c) 2002-2015 "Neo Technology,"
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

import org.neo4j.kernel.impl.store.NodeStore;
import org.neo4j.kernel.impl.store.record.NodeRecord;
import org.neo4j.kernel.impl.util.InstanceCache;

/**
 * Cursor for a single node.
 */
public class StoreSingleNodeCursor extends StoreAbstractNodeCursor
{
    private long nodeId;
    private InstanceCache<StoreSingleNodeCursor> instanceCache;

    public StoreSingleNodeCursor( NodeRecord nodeRecord,
            NodeStore nodeStore,
            StoreStatement storeStatement,
            InstanceCache<StoreSingleNodeCursor> instanceCache )
    {
        super( nodeRecord, nodeStore, storeStatement );
        this.instanceCache = instanceCache;
    }

    public StoreSingleNodeCursor init( long nodeId )
    {
        this.nodeId = nodeId;
        return this;
    }

    @Override
    public boolean next()
    {
        if ( nodeId != -1 )
        {
            try
            {
                nodeRecord.setId( nodeId );
                NodeRecord record = nodeStore.loadRecord( nodeId, this.nodeRecord );
                return record != null && record.inUse();
            }
            finally
            {
                nodeId = -1;
            }
        }

        return false;
    }

    @Override
    public void close()
    {
        instanceCache.accept( this );
    }
}
