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
package org.neo4j.kernel.impl.api.store;

import java.util.function.Consumer;

import org.neo4j.kernel.api.StatementConstants;
import org.neo4j.kernel.impl.locking.LockService;
import org.neo4j.kernel.impl.store.NeoStores;
import org.neo4j.kernel.impl.store.RecordCursors;
import org.neo4j.kernel.impl.store.record.NodeRecord;

import static org.neo4j.kernel.impl.store.record.RecordLoad.CHECK;

/**
 * Cursor for a single node.
 */
public class StoreSingleNodeCursor extends StoreAbstractNodeCursor
{
    private long nodeId;
    private final Consumer<StoreSingleNodeCursor> instanceCache;

    public StoreSingleNodeCursor( NodeRecord nodeRecord,
            NeoStores neoStores,
            StoreStatement storeStatement,
            Consumer<StoreSingleNodeCursor> instanceCache,
            RecordCursors cursors,
            LockService lockService )
    {
        super( nodeRecord, neoStores, storeStatement, cursors, lockService );
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
        if ( nodeId != StatementConstants.NO_SUCH_NODE )
        {
            try
            {
                return cursors.node().next( nodeId, nodeRecord, CHECK );
            }
            finally
            {
                nodeId = StatementConstants.NO_SUCH_NODE;
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
