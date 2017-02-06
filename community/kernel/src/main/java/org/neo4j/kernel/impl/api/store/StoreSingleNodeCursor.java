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

import java.util.Objects;
import java.util.function.Consumer;
import java.util.function.Supplier;

import org.neo4j.collection.primitive.Primitive;
import org.neo4j.collection.primitive.PrimitiveIntCollections;
import org.neo4j.collection.primitive.PrimitiveIntObjectMap;
import org.neo4j.collection.primitive.PrimitiveIntSet;
import org.neo4j.cursor.Cursor;
import org.neo4j.kernel.api.StatementConstants;
import org.neo4j.kernel.api.cursor.EntityItemHelper;
import org.neo4j.kernel.impl.locking.Lock;
import org.neo4j.kernel.impl.locking.LockService;
import org.neo4j.kernel.impl.store.InvalidRecordException;
import org.neo4j.kernel.impl.store.NeoStores;
import org.neo4j.kernel.impl.store.NodeLabelsField;
import org.neo4j.kernel.impl.store.RecordCursor;
import org.neo4j.kernel.impl.store.RecordCursors;
import org.neo4j.kernel.impl.store.RecordStore;
import org.neo4j.kernel.impl.store.RelationshipStore;
import org.neo4j.kernel.impl.store.record.DynamicRecord;
import org.neo4j.kernel.impl.store.record.NodeRecord;
import org.neo4j.kernel.impl.store.record.Record;
import org.neo4j.kernel.impl.store.record.RelationshipGroupRecord;
import org.neo4j.kernel.impl.util.IoPrimitiveUtils;
import org.neo4j.storageengine.api.DegreeItem;
import org.neo4j.storageengine.api.Direction;
import org.neo4j.storageengine.api.NodeItem;
import org.neo4j.storageengine.api.PropertyItem;
import org.neo4j.storageengine.api.RelationshipItem;

import static org.neo4j.collection.primitive.Primitive.intSet;
import static org.neo4j.kernel.impl.api.store.DegreeCounter.countRelationshipsInGroup;
import static org.neo4j.kernel.impl.locking.LockService.NO_LOCK_SERVICE;
import static org.neo4j.kernel.impl.store.record.Record.NO_NEXT_RELATIONSHIP;
import static org.neo4j.kernel.impl.store.record.RecordLoad.CHECK;
import static org.neo4j.kernel.impl.store.record.RecordLoad.FORCE;
import static org.neo4j.kernel.impl.util.Cursors.count;
import static org.neo4j.kernel.impl.util.IoPrimitiveUtils.safeCastLongToInt;

/**
 * Base cursor for nodes.
 */
public class StoreSingleNodeCursor extends EntityItemHelper implements Cursor<NodeItem>, NodeItem
{
    private static class NodeLabelView implements Supplier<PrimitiveIntSet>
    {
        private final RecordCursor<DynamicRecord> dynamicLabelRecordCursor;
        private long[] labels;

        NodeLabelView( RecordCursor<DynamicRecord> dynamicLabelRecordCursor )
        {
            this.dynamicLabelRecordCursor = dynamicLabelRecordCursor;
        }

        NodeLabelView load( NodeRecord nodeRecord )
        {
            if ( labels == null )
            {
                labels = NodeLabelsField.get( nodeRecord, dynamicLabelRecordCursor );
            }
            return this;
        }

        void clear()
        {
            labels = null;
        }

        boolean hasLabel( int labelId )
        {
            Objects.requireNonNull( labels );
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
        public PrimitiveIntSet get()
        {
            Objects.requireNonNull( labels );
            return PrimitiveIntCollections.asSet( labels, IoPrimitiveUtils::safeCastLongToInt );
        }
    }

    private final NodeLabelView labelView;
    private final NodeRecord nodeRecord;
    private final RelationshipStore relationshipStore;
    private final RecordStore<RelationshipGroupRecord> relationshipGroupStore;
    private final Consumer<StoreSingleNodeCursor> instanceCache;

    private final LockService lockService;
    private final RecordCursors recordCursors;
    private final NodeExploringCursors cursors;

    private long nodeId = StatementConstants.NO_SUCH_NODE;

    StoreSingleNodeCursor( NodeRecord nodeRecord, NeoStores neoStores, Consumer<StoreSingleNodeCursor> instanceCache,
            RecordCursors recordCursors, LockService lockService )
    {
        this.nodeRecord = nodeRecord;
        this.recordCursors = recordCursors;
        this.relationshipStore = neoStores.getRelationshipStore();
        this.relationshipGroupStore = neoStores.getRelationshipGroupStore();
        this.lockService = lockService;
        this.instanceCache = instanceCache;
        this.cursors =
                new NodeExploringCursors( recordCursors, lockService, relationshipStore, relationshipGroupStore );
        this.labelView = new NodeLabelView( recordCursors.label() );
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

        labelView.clear();
        return false;
    }

    @Override
    public void close()
    {
        labelView.clear();
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
        return labelView.load( nodeRecord ).get();
    }

    @Override
    public boolean hasLabel( int labelId )
    {
        return labelView.load( nodeRecord ).hasLabel( labelId );
    }

    private Lock shortLivedReadLock()
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

    @Override
    public Cursor<PropertyItem> properties()
    {
        return cursors.properties( nodeRecord.getNextProp(), shortLivedReadLock() );
    }

    @Override
    public Cursor<PropertyItem> property( int propertyKeyId )
    {
        return cursors.property( nodeRecord.getNextProp(), propertyKeyId, shortLivedReadLock() );
    }

    @Override
    public Cursor<RelationshipItem> relationships( Direction direction )
    {
        return cursors.relationships( nodeRecord.isDense(), nodeRecord.getNextRel(), nodeRecord.getId(), direction );
    }

    @Override
    public Cursor<RelationshipItem> relationships( Direction direction, int... relTypes )
    {
        return cursors.relationships( nodeRecord.isDense(), nodeRecord.getNextRel(), nodeRecord.getId(), direction,
                relTypes );
    }

    @Override
    public PrimitiveIntSet relationshipTypes()
    {
        PrimitiveIntSet set = intSet();
        if ( nodeRecord.isDense() )
        {
            RelationshipGroupRecord groupRecord = relationshipGroupStore.newRecord();
            for ( long id = nodeRecord.getNextRel(); id != NO_NEXT_RELATIONSHIP.intValue(); id = groupRecord.getNext() )
            {
                if ( recordCursors.relationshipGroup().next( id, groupRecord, FORCE ) )
                {
                    set.add( groupRecord.getType() );
                }
            }
        }
        else
        {
            relationships( Direction.BOTH ).forAll( relationship -> set.add( relationship.type() ) );
        }
        return set;
    }

    @Override
    public int degree( Direction direction )
    {
        if ( nodeRecord.isDense() )
        {
            return countRelationshipsInGroup( nodeRecord.getNextRel(), direction, null, nodeRecord,
                    relationshipStore.newRecord(), relationshipGroupStore.newRecord(), recordCursors );
        }
        else
        {
            return count( relationships( direction ) );
        }
    }

    @Override
    public int degree( Direction direction, int relType )
    {
        if ( nodeRecord.isDense() )
        {
            return countRelationshipsInGroup( nodeRecord.getNextRel(), direction, relType, nodeRecord,
                    relationshipStore.newRecord(), relationshipGroupStore.newRecord(), recordCursors );
        }
        else
        {
            return count( relationships( direction, relType ) );
        }
    }

    @Override
    public Cursor<DegreeItem> degrees()
    {
        return nodeRecord.isDense() ? cursors.degrees( nodeRecord.getNextRel(), nodeRecord, recordCursors )
                                    : cursors.degrees( buildDegreeMap() );
    }

    private PrimitiveIntObjectMap<int[]> buildDegreeMap()
    {
        PrimitiveIntObjectMap<int[]> currentDegrees = Primitive.intObjectMap( 5 );
        try( Cursor<RelationshipItem> relationships = relationships( Direction.BOTH ) )
        {
            while ( relationships.next() )
            {
                RelationshipItem rel = relationships.get();
                int[] byType = currentDegrees.get( rel.type() );
                if ( byType == null )
                {
                    currentDegrees.put( rel.type(), byType = new int[3] );
                }
                byType[directionOf( nodeRecord.getId(), rel.id(), rel.startNode(), rel.endNode() ).ordinal()]++;
            }
        }
        return currentDegrees;
    }

    @Override
    public boolean isDense()
    {
        return nodeRecord.isDense();
    }

    private Direction directionOf( long nodeId, long relationshipId, long startNode, long endNode )
    {
        if ( startNode == nodeId )
        {
            return endNode == nodeId ? Direction.BOTH : Direction.OUTGOING;
        }
        if ( endNode == nodeId )
        {
            return Direction.INCOMING;
        }
        throw new InvalidRecordException(
                "Node " + nodeId + " neither start nor end node of relationship " + relationshipId +
                        " with startNode:" + startNode + " and endNode:" + endNode );
    }
}
