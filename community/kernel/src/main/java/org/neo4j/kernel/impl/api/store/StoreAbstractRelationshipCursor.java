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

import java.io.IOException;

import org.neo4j.cursor.Cursor;
import org.neo4j.function.Disposable;
import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.kernel.impl.api.RelationshipVisitor;
import org.neo4j.kernel.impl.locking.Lock;
import org.neo4j.kernel.impl.locking.LockService;
import org.neo4j.kernel.impl.store.RelationshipStore;
import org.neo4j.kernel.impl.store.UnderlyingStorageException;
import org.neo4j.kernel.impl.store.record.Record;
import org.neo4j.kernel.impl.store.record.RecordLoad;
import org.neo4j.kernel.impl.store.record.RelationshipRecord;
import org.neo4j.storageengine.api.RelationshipItem;

import static org.neo4j.kernel.impl.locking.LockService.NO_LOCK_SERVICE;
import static org.neo4j.kernel.impl.store.record.Record.NO_NEXT_PROPERTY;
import static org.neo4j.kernel.impl.store.record.RecordLoad.FORCE;

/**
 * Base cursor for relationships.
 */
public abstract class StoreAbstractRelationshipCursor
        implements RelationshipVisitor<RuntimeException>, RelationshipItem, Cursor<RelationshipItem>, Disposable
{
    private final RelationshipRecord relationshipRecord;
    private final PageCursor cursor;
    private final RelationshipStore relationshipStore;
    private final LockService lockService;
    protected boolean fetched;

    StoreAbstractRelationshipCursor( RelationshipStore relationshipStore, LockService lockService )
    {
        this.relationshipStore = relationshipStore;
        this.relationshipRecord = relationshipStore.newRecord();
        this.cursor = relationshipStore.newPageCursor();
        this.lockService = lockService;
    }

    @Override
    public final RelationshipItem get()
    {
        if ( !fetched )
        {
            throw new IllegalStateException();
        }

        return this;
    }

    @Override
    public final boolean next()
    {
        return fetched = fetchNext();
    }

    protected abstract boolean fetchNext();

    @Override
    public final long id()
    {
        return relationshipRecord.getId();
    }

    @Override
    public final int type()
    {
        return relationshipRecord.getType();
    }

    @Override
    public final long startNode()
    {
        return relationshipRecord.getFirstNode();
    }

    @Override
    public final long endNode()
    {
        return relationshipRecord.getSecondNode();
    }

    @Override
    public final long otherNode( long nodeId )
    {
        return relationshipRecord.getFirstNode() == nodeId ? relationshipRecord.getSecondNode() : relationshipRecord.getFirstNode();
    }

    @Override
    public final void visit( long relId, int type, long startNode, long endNode ) throws RuntimeException
    {
        relationshipRecord.setId( relId );
        relationshipRecord.setType( type );
        relationshipRecord.setFirstNode( startNode );
        relationshipRecord.setSecondNode( endNode );
        relationshipRecord.setNextProp( NO_NEXT_PROPERTY.longValue() );
    }

    @Override
    public final long nextPropertyId()
    {
        return relationshipRecord.getNextProp();
    }

    RelationshipRecord readRecord( long id, RecordLoad mode )
    {
        try
        {
            relationshipRecord.clear();
            relationshipStore.readIntoRecord( id, relationshipRecord, mode, cursor );
            return relationshipRecord;
        }
        catch ( IOException e )
        {
            throw new UnderlyingStorageException( e );
        }
    }

    @Override
    public final Lock lock()
    {
        Lock lock = lockService.acquireRelationshipLock( relationshipRecord.getId(), LockService.LockType.READ_LOCK );
        if ( lockService != NO_LOCK_SERVICE )
        {
            boolean success = false;
            try
            {
                RelationshipRecord record = readRecord( relationshipRecord.getId(), FORCE );
                // It's safer to re-read the relationship record here, specifically nextProp, after acquiring the lock
                if ( !record.inUse() )
                {
                    // So it looks like the relationship has been deleted. The current behavior of
                    // RelationshipStore#fillRecord w/ FORCE is to only set the inUse field on loading an unused record.
                    // This should (and will) change to be more of a centralized behavior by the stores. Anyway,
                    // setting this pointer to the primitive equivalent of null the property cursor will just look
                    // empty from the outside and the releasing of the lock will be done as usual.
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
        }
        return lock;
    }

    @Override
    public void close()
    {
        fetched = false;
    }

    @Override
    public void dispose()
    {
    }
}
