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

import org.neo4j.collection.primitive.PrimitiveLongIterator;
import org.neo4j.kernel.impl.locking.LockService;
import org.neo4j.kernel.impl.store.RecordCursors;
import org.neo4j.kernel.impl.store.record.RelationshipRecord;
import org.neo4j.kernel.impl.util.InstanceCache;
import org.neo4j.storageengine.api.txstate.ReadableTransactionState;

import static org.neo4j.collection.primitive.PrimitiveLongCollections.toPrimitiveIterator;
import static org.neo4j.kernel.impl.store.record.RecordLoad.CHECK;

/**
 * Cursor for iterating a set of relationships.
 */
public class StoreIteratorRelationshipCursor extends StoreAbstractIteratorRelationshipCursor
{
    private PrimitiveLongIterator iterator;
    private final InstanceCache<StoreIteratorRelationshipCursor> instanceCache;

    StoreIteratorRelationshipCursor( RelationshipRecord relationshipRecord,
            InstanceCache<StoreIteratorRelationshipCursor> instanceCache, RecordCursors cursors,
            LockService lockService )
    {
        super( relationshipRecord, cursors, lockService );
        this.instanceCache = instanceCache;
    }

    public StoreIteratorRelationshipCursor init( PrimitiveLongIterator iterator, ReadableTransactionState state )
    {
        super.init( state, addedRelationships( state ) );
        this.iterator = iterator;
        return this;
    }

    private PrimitiveLongIterator addedRelationships( ReadableTransactionState state )
    {
        return state == null ? null : toPrimitiveIterator( state.addedAndRemovedRelationships().getAdded().iterator() );
    }

    @Override
    protected boolean doFetchNext()
    {
        while ( iterator != null && iterator.hasNext() )
        {
            if ( relationshipRecordCursor.next( iterator.next(), relationshipRecord, CHECK ) )
            {
                return true;
            }
        }

        return false;
    }

    @Override
    public void close()
    {
        super.close();
        iterator = null;
        instanceCache.accept( this );
    }
}
