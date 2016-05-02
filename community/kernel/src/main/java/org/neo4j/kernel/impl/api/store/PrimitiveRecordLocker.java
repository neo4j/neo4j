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

import org.neo4j.kernel.impl.locking.Lock;
import org.neo4j.kernel.impl.locking.LockService;
import org.neo4j.kernel.impl.store.NodeStore;
import org.neo4j.kernel.impl.store.RelationshipStore;
import org.neo4j.kernel.impl.store.record.NodeRecord;
import org.neo4j.kernel.impl.store.record.Record;
import org.neo4j.kernel.impl.store.record.RelationshipRecord;

import static org.neo4j.kernel.impl.locking.LockService.NO_LOCK;
import static org.neo4j.kernel.impl.locking.LockService.NO_LOCK_SERVICE;
import static org.neo4j.kernel.impl.store.record.RecordLoad.FORCE;

final class PrimitiveRecordLocker
{
    private PrimitiveRecordLocker()
    {
        throw new AssertionError( "Not for instantiation!" );
    }

    /**
     * Acquires a read lock for the given node and then re-reads the record to get consistent data.
     * This method should be called <strong>before</strong> accessing other fields of the node record.
     *
     * @param lockService the lock service to acquire locks from.
     * @param record the node to lock.
     * @param store the node store.
     * @return the {@link Lock} that must be closed after all related data have been read.
     */
    static Lock shortLivedReadLock( LockService lockService, NodeRecord record, NodeStore store )
    {
        if ( lockService == NO_LOCK_SERVICE )
        {
            return NO_LOCK;
        }

        long id = record.getId();
        Lock lock = lockService.acquireNodeLock( id, LockService.LockType.READ_LOCK );
        boolean success = false;
        try
        {
            // It's safer to re-read the node record here, specifically nextProp, after acquiring the lock
            store.loadRecord( id, record );
            if ( !record.inUse() )
            {
                // So it looks like the node has been deleted. The current behavior of NodeStore#loadRecord
                // is to only set the inUse field on loading an unused record. This should (and will)
                // change to be more of a centralized behavior by the stores. Anyway, setting this pointer
                // to the primitive equivalent of null the property cursor will just look empty from the
                // outside and the releasing of the lock will be done as usual.
                record.setNextProp( Record.NO_NEXT_PROPERTY.intValue() );
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
        return lock;
    }

    /**
     * Acquires a read lock for the given relationship and then re-reads the record to get consistent data.
     * This method should be called <strong>before</strong> accessing other fields of the relationship record.
     *
     * @param lockService the lock service to acquire locks from.
     * @param record the relationship to lock.
     * @param store the relationship store.
     * @return the {@link Lock} that must be closed after all related data have been read.
     */
    static Lock shortLivedReadLock( LockService lockService, RelationshipRecord record, RelationshipStore store )
    {
        if ( lockService == NO_LOCK_SERVICE )
        {
            return NO_LOCK;
        }

        long id = record.getId();
        Lock lock = lockService.acquireRelationshipLock( id, LockService.LockType.READ_LOCK );
        boolean success = false;
        try
        {
            // It's safer to re-read the relationship record here, specifically nextProp, after acquiring the lock
            store.fillRecord( id, record, FORCE );
            if ( !record.inUse() )
            {
                // So it looks like the node has been deleted. The current behavior of RelationshipStore#fillRecord
                // w/ FORCE is to only set the inUse field on loading an unused record. This should (and will)
                // change to be more of a centralized behavior by the stores. Anyway, setting this pointer
                // to the primitive equivalent of null the property cursor will just look empty from the
                // outside and the releasing of the lock will be done as usual.
                record.setNextProp( Record.NO_NEXT_PROPERTY.intValue() );
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
        return lock;
    }
}
