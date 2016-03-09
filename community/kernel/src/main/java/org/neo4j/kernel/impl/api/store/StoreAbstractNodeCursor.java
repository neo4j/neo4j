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

import java.util.function.IntSupplier;

import org.neo4j.collection.primitive.Primitive;
import org.neo4j.collection.primitive.PrimitiveIntIterator;
import org.neo4j.collection.primitive.PrimitiveIntObjectMap;
import org.neo4j.collection.primitive.PrimitiveIntSet;
import org.neo4j.cursor.Cursor;
import org.neo4j.cursor.IntValue;
import org.neo4j.kernel.api.cursor.NodeItemHelper;
import org.neo4j.kernel.impl.locking.Lock;
import org.neo4j.kernel.impl.locking.LockService;
import org.neo4j.kernel.impl.store.InvalidRecordException;
import org.neo4j.kernel.impl.store.NeoStores;
import org.neo4j.kernel.impl.store.NodeLabelsField;
import org.neo4j.kernel.impl.store.NodeStore;
import org.neo4j.kernel.impl.store.RecordStore;
import org.neo4j.kernel.impl.store.RelationshipStore;
import org.neo4j.kernel.impl.store.record.NodeRecord;
import org.neo4j.kernel.impl.store.record.Record;
import org.neo4j.kernel.impl.store.record.RelationshipGroupRecord;
import org.neo4j.kernel.impl.store.record.RelationshipRecord;
import org.neo4j.kernel.impl.util.InstanceCache;
import org.neo4j.storageengine.api.DegreeItem;
import org.neo4j.storageengine.api.Direction;
import org.neo4j.storageengine.api.LabelItem;
import org.neo4j.storageengine.api.NodeItem;
import org.neo4j.storageengine.api.PropertyItem;
import org.neo4j.storageengine.api.RelationshipItem;

import static org.neo4j.kernel.impl.locking.LockService.NO_LOCK_SERVICE;
import static org.neo4j.kernel.impl.store.record.RecordLoad.CHECK;
import static org.neo4j.kernel.impl.store.record.RecordLoad.NORMAL;

/**
 * Base cursor for nodes.
 */
public abstract class StoreAbstractNodeCursor extends NodeItemHelper implements Cursor<NodeItem>, NodeItem
{
    protected final NodeRecord nodeRecord;
    protected NodeStore nodeStore;
    protected RecordStore<RelationshipGroupRecord> relationshipGroupStore;
    protected RelationshipStore relationshipStore;
    protected final LockService lockService;
    protected StoreStatement storeStatement;

    private InstanceCache<StoreLabelCursor> labelCursor;
    private InstanceCache<StoreSingleLabelCursor> singleLabelCursor;
    private InstanceCache<StoreNodeRelationshipCursor> nodeRelationshipCursor;
    private InstanceCache<StoreSinglePropertyCursor> singlePropertyCursor;
    private InstanceCache<StorePropertyCursor> allPropertyCursor;

    public StoreAbstractNodeCursor( NodeRecord nodeRecord,
            final NeoStores neoStores,
            final StoreStatement storeStatement,
            final LockService lockService )
    {
        this.nodeRecord = nodeRecord;
        this.nodeStore = neoStores.getNodeStore();
        this.relationshipStore = neoStores.getRelationshipStore();
        this.relationshipGroupStore = neoStores.getRelationshipGroupStore();
        this.storeStatement = storeStatement;
        this.lockService = lockService;

        labelCursor = new InstanceCache<StoreLabelCursor>()
        {
            @Override
            protected StoreLabelCursor create()
            {
                return new StoreLabelCursor( this );
            }
        };
        singleLabelCursor = new InstanceCache<StoreSingleLabelCursor>()
        {
            @Override
            protected StoreSingleLabelCursor create()
            {
                return new StoreSingleLabelCursor( this );
            }
        };
        nodeRelationshipCursor = new InstanceCache<StoreNodeRelationshipCursor>()
        {
            @Override
            protected StoreNodeRelationshipCursor create()
            {
                return new StoreNodeRelationshipCursor( new RelationshipRecord( -1 ),
                        neoStores,
                        new RelationshipGroupRecord( -1 ), storeStatement, this, lockService );
            }
        };
        singlePropertyCursor = new InstanceCache<StoreSinglePropertyCursor>()
        {
            @Override
            protected StoreSinglePropertyCursor create()
            {
                return new StoreSinglePropertyCursor( neoStores.getPropertyStore(), this );
            }
        };
        allPropertyCursor = new InstanceCache<StorePropertyCursor>()
        {
            @Override
            protected StorePropertyCursor create()
            {
                return new StorePropertyCursor( neoStores.getPropertyStore(), this );
            }
        };
    }

    @Override
    public NodeItem get()
    {
        return this;
    }

    @Override
    public long id()
    {
        return nodeRecord.getId();
    }

    @Override
    public Cursor<LabelItem> labels()
    {
        return labelCursor.get().init( NodeLabelsField.get( nodeRecord, nodeStore ) );
    }

    @Override
    public Cursor<LabelItem> label( int labelId )
    {
        return singleLabelCursor.get().init( NodeLabelsField.get( nodeRecord, nodeStore ), labelId );
    }

    /**
     * Acquires a read lock for the node in this cursor and then re-reads the record to get consistent data.
     * This method should be called <strong>before</strong> accessing other fields of the entity record.
     *
     * @return the {@link Lock} that must be closed after all related data have been read.
     */
    private Lock shortLivedReadLock()
    {
        Lock lock = lockService.acquireNodeLock( nodeRecord.getId(), LockService.LockType.READ_LOCK );
        if ( lockService != NO_LOCK_SERVICE )
        {
            boolean success = false;
            try
            {
                // It's safer to re-read the node record here, specifically nextProp, after acquiring the lock
                if ( !nodeStore.getRecord( nodeRecord.getId(), nodeRecord, CHECK ).inUse() )
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
        Lock lock = shortLivedReadLock();
        return allPropertyCursor.get().init( nodeRecord.getNextProp(), lock );
    }

    @Override
    public Cursor<PropertyItem> property( int propertyKeyId )
    {
        Lock lock = shortLivedReadLock();
        return singlePropertyCursor.get().init( nodeRecord.getNextProp(), propertyKeyId, lock );
    }

    @Override
    public Cursor<RelationshipItem> relationships( Direction direction )
    {
        return nodeRelationshipCursor.get().init( nodeRecord.isDense(), nodeRecord.getNextRel(), nodeRecord.getId(),
                direction, null );
    }

    @Override
    public Cursor<RelationshipItem> relationships( Direction direction, int... relTypes )
    {
        return nodeRelationshipCursor.get().init( nodeRecord.isDense(), nodeRecord.getNextRel(), nodeRecord.getId(),
                direction, relTypes );
    }

    @Override
    public Cursor<IntSupplier> relationshipTypes()
    {
        if ( nodeRecord.isDense() )
        {
            return new Cursor<IntSupplier>()
            {
                private long groupId = nodeRecord.getNextRel();
                private final IntValue value = new IntValue();
                private final RelationshipGroupRecord group = relationshipGroupStore.newRecord();

                @Override
                public boolean next()
                {
                    if ( groupId == Record.NO_NEXT_RELATIONSHIP.intValue() )
                    {
                        return false;
                    }

                    relationshipGroupStore.getRecord( groupId, group, NORMAL );
                    try
                    {
                        value.setValue( group.getType() );
                        return true;
                    }
                    finally
                    {
                        groupId = group.getNext();
                    }
                }

                @Override
                public void close()
                {
                }

                @Override
                public IntSupplier get()
                {
                    return value;
                }
            };
        }
        else
        {
            final Cursor<RelationshipItem> relationships = relationships( Direction.BOTH );
            return new Cursor<IntSupplier>()
            {
                private final PrimitiveIntSet foundTypes = Primitive.intSet( 5 );
                private final IntValue value = new IntValue();

                @Override
                public boolean next()
                {
                    while ( relationships.next() )
                    {
                        if ( !foundTypes.contains( relationships.get().type() ) )
                        {
                            foundTypes.add( relationships.get().type() );
                            value.setValue( relationships.get().type() );
                            return true;
                        }
                    }

                    return false;
                }

                @Override
                public void close()
                {
                }

                @Override
                public IntSupplier get()
                {
                    return value;
                }
            };
        }
    }

    @Override
    public int degree( Direction direction )
    {
        if ( nodeRecord.isDense() )
        {
            long groupId = nodeRecord.getNextRel();
            long count = 0;
            RelationshipGroupRecord group = relationshipGroupStore.newRecord();
            while ( groupId != Record.NO_NEXT_RELATIONSHIP.intValue() )
            {
                relationshipGroupStore.getRecord( groupId, group, NORMAL );
                count += nodeDegreeByDirection( group, direction );
                groupId = group.getNext();
            }
            return (int) count;
        }
        else
        {
            try ( Cursor<RelationshipItem> relationship = relationships( direction ) )
            {
                int count = 0;
                while ( relationship.next() )
                {
                    count++;
                }
                return count;
            }
        }
    }

    @Override
    public int degree( Direction direction, int relType )
    {
        if ( nodeRecord.isDense() )
        {
            long groupId = nodeRecord.getNextRel();
            RelationshipGroupRecord group = relationshipGroupStore.newRecord();
            while ( groupId != Record.NO_NEXT_RELATIONSHIP.intValue() )
            {
                relationshipGroupStore.getRecord( groupId, group, NORMAL );
                if ( group.getType() == relType )
                {
                    return (int) nodeDegreeByDirection( group, direction );
                }
                groupId = group.getNext();
            }
            return 0;
        }
        else
        {
            try ( Cursor<RelationshipItem> relationship = relationships( direction, relType ) )
            {
                int count = 0;
                while ( relationship.next() )
                {
                    count++;
                }
                return count;
            }
        }
    }

    @Override
    public Cursor<DegreeItem> degrees()
    {
        if ( nodeRecord.isDense() )
        {
            long groupId = nodeRecord.getNextRel();
            return new DegreeItemDenseCursor( groupId );
        }
        else
        {
            final PrimitiveIntObjectMap<int[]> degrees = Primitive.intObjectMap( 5 );

            try ( Cursor<RelationshipItem> relationship = relationships( Direction.BOTH ) )
            {
                while ( relationship.next() )
                {
                    RelationshipItem rel = relationship.get();

                    int[] byType = degrees.get( rel.type() );
                    if ( byType == null )
                    {
                        degrees.put( rel.type(), byType = new int[3] );
                    }
                    byType[directionOf( nodeRecord.getId(), rel.id(), rel.startNode(), rel.endNode() ).ordinal()]++;
                }
            }

            final PrimitiveIntIterator keys = degrees.iterator();

            return new DegreeItemIterator( keys, degrees );
        }
    }

    @Override
    public boolean isDense()
    {
        return nodeRecord.isDense();
    }

    private long nodeDegreeByDirection( RelationshipGroupRecord group, Direction direction )
    {
        long loopCount = countByFirstPrevPointer( group.getFirstLoop() );
        switch ( direction )
        {
            case OUTGOING:
                return countByFirstPrevPointer( group.getFirstOut() ) + loopCount;
            case INCOMING:
                return countByFirstPrevPointer( group.getFirstIn() ) + loopCount;
            case BOTH:
                return countByFirstPrevPointer( group.getFirstOut() ) +
                        countByFirstPrevPointer( group.getFirstIn() ) + loopCount;
            default:
                throw new IllegalArgumentException( direction.name() );
        }
    }

    private long countByFirstPrevPointer( long relationshipId )
    {
        if ( relationshipId == Record.NO_NEXT_RELATIONSHIP.intValue() )
        {
            return 0;
        }
        RelationshipRecord record = relationshipStore.getRecord( relationshipId,
                relationshipStore.newRecord(), NORMAL );
        if ( record.getFirstNode() == nodeRecord.getId() )
        {
            return record.getFirstPrevRel();
        }
        if ( record.getSecondNode() == nodeRecord.getId() )
        {
            return record.getSecondPrevRel();
        }
        throw new InvalidRecordException( "Node " + nodeRecord.getId() + " neither start nor end node of " + record );
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
        throw new InvalidRecordException( "Node " + nodeId + " neither start nor end node of relationship " +
                relationshipId + " with startNode:" + startNode + " and endNode:" + endNode );
    }

    private static class DegreeItemIterator implements Cursor<DegreeItem>, DegreeItem
    {
        private final PrimitiveIntObjectMap<int[]> degrees;
        private PrimitiveIntIterator keys;

        private int type;
        private int outgoing;
        private int incoming;

        public DegreeItemIterator( PrimitiveIntIterator keys, PrimitiveIntObjectMap<int[]> degrees )
        {
            this.keys = keys;
            this.degrees = degrees;
        }

        @Override
        public void close()
        {
            keys = null;
        }

        @Override
        public int type()
        {
            return type;
        }

        @Override
        public long outgoing()
        {
            return outgoing;
        }

        @Override
        public long incoming()
        {
            return incoming;
        }

        @Override
        public DegreeItem get()
        {
            if ( keys == null )
            {
                throw new IllegalStateException();
            }

            return this;
        }

        @Override
        public boolean next()
        {
            if ( keys != null && keys.hasNext() )
            {
                type = keys.next();
                int[] degreeValues = degrees.get( type );
                outgoing = degreeValues[0] + degreeValues[2];
                incoming = degreeValues[1] + degreeValues[2];

                return true;
            }
            keys = null;
            return false;
        }
    }

    private class DegreeItemDenseCursor implements Cursor<DegreeItem>, DegreeItem
    {
        private long groupId;

        private int type;
        private long outgoing;
        private long incoming;
        private final RelationshipGroupRecord group = relationshipGroupStore.newRecord();

        public DegreeItemDenseCursor( long groupId )
        {
            this.groupId = groupId;
        }

        @Override
        public boolean next()
        {
            if ( groupId != Record.NO_NEXT_RELATIONSHIP.intValue() )
            {
                relationshipGroupStore.getRecord( groupId, group, NORMAL );
                this.type = group.getType();
                long loop = countByFirstPrevPointer( group.getFirstLoop() );
                outgoing = countByFirstPrevPointer( group.getFirstOut() ) + loop;
                incoming = countByFirstPrevPointer( group.getFirstIn() ) + loop;
                groupId = group.getNext();

                return true;
            }
            return false;
        }

        @Override
        public void close()
        {
        }

        @Override
        public DegreeItem get()
        {
            return this;
        }

        @Override
        public int type()
        {
            return type;
        }

        @Override
        public long outgoing()
        {
            return outgoing;
        }

        @Override
        public long incoming()
        {
            return incoming;
        }
    }
}
