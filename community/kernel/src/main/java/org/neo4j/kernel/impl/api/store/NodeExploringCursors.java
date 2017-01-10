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

import org.neo4j.collection.primitive.PrimitiveIntObjectMap;
import org.neo4j.cursor.Cursor;
import org.neo4j.cursor.IntCursor;
import org.neo4j.kernel.impl.locking.Lock;
import org.neo4j.kernel.impl.locking.LockService;
import org.neo4j.kernel.impl.store.RecordCursors;
import org.neo4j.kernel.impl.store.RecordStore;
import org.neo4j.kernel.impl.store.RelationshipStore;
import org.neo4j.kernel.impl.store.record.NodeRecord;
import org.neo4j.kernel.impl.store.record.RelationshipGroupRecord;
import org.neo4j.kernel.impl.util.InstanceCache;
import org.neo4j.storageengine.api.DegreeItem;
import org.neo4j.storageengine.api.Direction;
import org.neo4j.storageengine.api.PropertyItem;
import org.neo4j.storageengine.api.RelationshipItem;

class NodeExploringCursors
{
    private final InstanceCache<StoreLabelCursor> labelCursorCache;
    private final InstanceCache<StoreSingleLabelCursor> singleLabelCursorCache;
    private final InstanceCache<StoreNodeRelationshipCursor> nodeRelationshipCursorCache;
    private final InstanceCache<StoreSinglePropertyCursor> singlePropertyCursorCache;
    private final InstanceCache<StorePropertyCursor> propertyCursorCache;

    private final RelationshipStore relationshipStore;
    private final RecordStore<RelationshipGroupRecord> relationshipGroupStore;

    NodeExploringCursors( final RecordCursors cursors, final LockService lockService,
            final RelationshipStore relationshipStore,
            final RecordStore<RelationshipGroupRecord> relationshipGroupStore )
    {
        labelCursorCache = new InstanceCache<StoreLabelCursor>()
        {
            @Override
            protected StoreLabelCursor create()
            {
                return new StoreLabelCursor( cursors.label(), labelCursorCache );
            }
        };
        singleLabelCursorCache = new InstanceCache<StoreSingleLabelCursor>()
        {
            @Override
            protected StoreSingleLabelCursor create()
            {
                return new StoreSingleLabelCursor( cursors.label(), singleLabelCursorCache );
            }
        };
        this.relationshipStore = relationshipStore;
        this.relationshipGroupStore = relationshipGroupStore;
        nodeRelationshipCursorCache = new InstanceCache<StoreNodeRelationshipCursor>()
        {
            @Override
            protected StoreNodeRelationshipCursor create()
            {
                return new StoreNodeRelationshipCursor( relationshipStore.newRecord(),
                        relationshipGroupStore.newRecord(), nodeRelationshipCursorCache, cursors, lockService );
            }
        };
        singlePropertyCursorCache = new InstanceCache<StoreSinglePropertyCursor>()
        {
            @Override
            protected StoreSinglePropertyCursor create()
            {
                return new StoreSinglePropertyCursor( cursors, singlePropertyCursorCache );
            }
        };
        propertyCursorCache = new InstanceCache<StorePropertyCursor>()
        {
            @Override
            protected StorePropertyCursor create()
            {
                return new StorePropertyCursor( cursors, propertyCursorCache );
            }
        };
    }

    public Cursor<PropertyItem> properties( long nextProp, Lock lock )
    {
        return propertyCursorCache.get().init( nextProp, lock );
    }

    public Cursor<PropertyItem> property( long nextProp, int propertyKeyId, Lock lock )
    {
        return singlePropertyCursorCache.get().init( nextProp, propertyKeyId, lock );
    }

    public IntCursor labels( NodeRecord nodeRecord )
    {
        return labelCursorCache.get().init( nodeRecord );
    }

    public IntCursor label( NodeRecord nodeRecord, int labelId )
    {
        return singleLabelCursorCache.get().init( nodeRecord, labelId );
    }

    public Cursor<RelationshipItem> relationships( boolean dense, long nextRel, long id, Direction direction )
    {
        return nodeRelationshipCursorCache.get().init( dense, nextRel, id, direction );
    }

    public Cursor<RelationshipItem> relationships( boolean dense, long nextRel, long id, Direction direction,
            int... relTypes )
    {
        return nodeRelationshipCursorCache.get().init( dense, nextRel, id, direction, relTypes );
    }

    public Cursor<DegreeItem> degrees( PrimitiveIntObjectMap<int[]> degrees )
    {
        return new DegreeItemCursor( degrees );
    }

    public Cursor<DegreeItem> degrees( long groupId, NodeRecord nodeRecord, RecordCursors recordCursors )
    {
        return new DegreeItemDenseCursor( groupId, nodeRecord, relationshipGroupStore.newRecord(),
                relationshipStore.newRecord(), recordCursors );
    }
}
