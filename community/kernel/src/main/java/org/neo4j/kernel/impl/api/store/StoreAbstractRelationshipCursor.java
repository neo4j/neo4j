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

import org.neo4j.cursor.Cursor;
import org.neo4j.kernel.api.cursor.EntityItemHelper;
import org.neo4j.kernel.impl.locking.Lock;
import org.neo4j.kernel.impl.locking.LockService;
import org.neo4j.kernel.impl.store.NeoStores;
import org.neo4j.kernel.impl.store.RecordStore;
import org.neo4j.kernel.impl.store.RelationshipStore;
import org.neo4j.kernel.impl.store.record.Record;
import org.neo4j.kernel.impl.store.record.RelationshipGroupRecord;
import org.neo4j.kernel.impl.store.record.RelationshipRecord;
import org.neo4j.kernel.impl.util.InstanceCache;
import org.neo4j.storageengine.api.PropertyItem;
import org.neo4j.storageengine.api.RelationshipItem;

import static org.neo4j.kernel.impl.locking.LockService.NO_LOCK_SERVICE;
import static org.neo4j.kernel.impl.store.record.RecordLoad.CHECK;

/**
 * Base cursor for relationships.
 */
public abstract class StoreAbstractRelationshipCursor extends EntityItemHelper
        implements Cursor<RelationshipItem>, RelationshipItem
{
    protected final RelationshipRecord relationshipRecord;
    protected final RelationshipStore relationshipStore;
    protected final RecordStore<RelationshipGroupRecord> relationshipGroupStore;
    private final LockService lockService;
    protected StoreStatement storeStatement;

    private InstanceCache<StoreSinglePropertyCursor> singlePropertyCursor;
    private InstanceCache<StorePropertyCursor> allPropertyCursor;

    public StoreAbstractRelationshipCursor( RelationshipRecord relationshipRecord, final NeoStores neoStores,
            StoreStatement storeStatement, LockService lockService )
    {
        this.lockService = lockService;
        this.relationshipStore = neoStores.getRelationshipStore();
        this.relationshipGroupStore = neoStores.getRelationshipGroupStore();
        this.relationshipRecord = relationshipRecord;

        this.storeStatement = storeStatement;

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
    public RelationshipItem get()
    {
        return this;
    }

    @Override
    public long id()
    {
        return relationshipRecord.getId();
    }

    @Override
    public int type()
    {
        return relationshipRecord.getType();
    }

    @Override
    public long startNode()
    {
        return relationshipRecord.getFirstNode();
    }

    @Override
    public long endNode()
    {
        return relationshipRecord.getSecondNode();
    }

    @Override
    public long otherNode( long nodeId )
    {
        return relationshipRecord.getFirstNode() == nodeId ?
                relationshipRecord.getSecondNode() : relationshipRecord.getFirstNode();
    }

    /**
     * Acquires a read lock for the relationship in this cursor and then re-reads the record to get consistent data.
     * This method should be called <strong>before</strong> accessing other fields of the entity record.
     *
     * @return the {@link Lock} that must be closed after all related data have been read.
     */
    private Lock shortLivedReadLock()
    {
        Lock lock = lockService.acquireRelationshipLock( relationshipRecord.getId(), LockService.LockType.READ_LOCK );
        if ( lockService != NO_LOCK_SERVICE )
        {
            boolean success = true;
            try
            {
                // It's safer to re-read the relationship record here, specifically nextProp, after acquiring the lock
                if ( !relationshipStore.getRecord( relationshipRecord.getId(), relationshipRecord, CHECK ).inUse() )
                {
                    // So it looks like the node has been deleted. The current behavior of RelationshipStore#fillRecord
                    // w/ FORCE is to only set the inUse field on loading an unused record. This should (and will)
                    // change to be more of a centralized behavior by the stores. Anyway, setting this pointer
                    // to the primitive equivalent of null the property cursor will just look empty from the
                    // outside and the releasing of the lock will be done as usual.
                    relationshipRecord.setNextProp( Record.NO_NEXT_PROPERTY.intValue() );
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
        return allPropertyCursor.get().init( relationshipRecord.getNextProp(), lock );
    }

    @Override
    public Cursor<PropertyItem> property( int propertyKeyId )
    {
        Lock lock = shortLivedReadLock();
        return singlePropertyCursor.get().init( relationshipRecord.getNextProp(), propertyKeyId, lock );
    }
}
