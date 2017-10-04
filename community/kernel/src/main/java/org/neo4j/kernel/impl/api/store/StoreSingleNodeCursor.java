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
package org.neo4j.kernel.impl.api.store;

import java.util.function.Consumer;

import org.neo4j.collection.primitive.PrimitiveIntCollections;
import org.neo4j.collection.primitive.PrimitiveIntSet;
import org.neo4j.cursor.Cursor;
import org.neo4j.helpers.Numbers;
import org.neo4j.kernel.api.StatementConstants;
import org.neo4j.kernel.impl.locking.Lock;
import org.neo4j.kernel.impl.locking.LockService;
import org.neo4j.kernel.impl.store.NodeLabelsField;
import org.neo4j.kernel.impl.store.RecordCursors;
import org.neo4j.kernel.impl.store.record.NodeRecord;
import org.neo4j.kernel.impl.store.record.Record;
import org.neo4j.storageengine.api.NodeItem;

import static org.neo4j.helpers.Numbers.safeCastLongToInt;
import static org.neo4j.kernel.impl.locking.LockService.NO_LOCK_SERVICE;
import static org.neo4j.kernel.impl.store.record.RecordLoad.CHECK;

/**
 * Base cursor for nodes.
 */
public class StoreSingleNodeCursor implements Cursor<NodeItem>, NodeItem
{
    private final NodeRecord nodeRecord;
    private final Consumer<StoreSingleNodeCursor> instanceCache;

    private final LockService lockService;
    private final RecordCursors recordCursors;

    private long nodeId = StatementConstants.NO_SUCH_NODE;
    private long[] labels;

    public StoreSingleNodeCursor( NodeRecord nodeRecord, Consumer<StoreSingleNodeCursor> instanceCache,
            RecordCursors recordCursors, LockService lockService )
    {
        this.nodeRecord = nodeRecord;
        this.recordCursors = recordCursors;
        this.lockService = lockService;
        this.instanceCache = instanceCache;
    }

    public StoreSingleNodeCursor init( long nodeId )
    {
        this.nodeId = nodeId;
        return this;
    }

    @Override
    public NodeItem get()
    {
        return this;
    }

    @Override
    public boolean next()
    {
        labels = null;
        if ( nodeId != StatementConstants.NO_SUCH_NODE )
        {
            try
            {
                return recordCursors.node().next( nodeId, nodeRecord, CHECK );
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
        labels = null;
        nodeRecord.clear();
        instanceCache.accept( this );
    }

    @Override
    public long id()
    {
        return nodeRecord.getId();
    }

    @Override
    public PrimitiveIntSet labels()
    {
        ensureLabels();
        return PrimitiveIntCollections.asSet( labels, Numbers::safeCastLongToInt );
    }

    private void ensureLabels()
    {
        if ( labels == null )
        {
            labels = NodeLabelsField.get( nodeRecord, recordCursors.label() );
        }
    }

    @Override
    public boolean hasLabel( int labelId )
    {
        ensureLabels();
        for ( long label : labels )
        {
            if ( safeCastLongToInt( label ) == labelId )
            {
                return true;
            }
        }
        return false;
    }

    @Override
    public boolean isDense()
    {
        return nodeRecord.isDense();
    }

    @Override
    public long nextGroupId()
    {
        assert isDense();
        return nextRelationshipId();
    }

    @Override
    public long nextRelationshipId()
    {
        return nodeRecord.getNextRel();
    }

    @Override
    public long nextPropertyId()
    {
        return nodeRecord.getNextProp();
    }

    @Override
    public Lock lock()
    {
        Lock lock = lockService.acquireNodeLock( nodeRecord.getId(), LockService.LockType.READ_LOCK );
        if ( lockService != NO_LOCK_SERVICE )
        {
            boolean success = false;
            try
            {
                // It's safer to re-read the node record here, specifically nextProp, after acquiring the lock
                if ( !recordCursors.node().next( nodeRecord.getId(), nodeRecord, CHECK ) )
                {
                    // So it looks like the node has been deleted. The current behavior of NodeStore#loadRecord
                    // is to only set the inUse field on loading an unused record. This should (and will)
                    // change to be more of a centralized behavior by the stores. Anyway, setting this pointer
                    // to the primitive equivalent of null the property cursor will just look empty from the
                    // outside and the releasing of the lock will be done as usual.
                    nodeRecord.setNextProp( Record.NO_NEXT_PROPERTY.intValue() );
                }
                success = true;
            }
            finally
            {
                if ( !success )
                {
                    lock.release();
                }
            }
        }
        return lock;
    }
}
