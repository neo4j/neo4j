/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
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
package org.neo4j.kernel.impl.storageengine.impl.recordstorage;

import org.eclipse.collections.api.iterator.LongIterator;

import org.neo4j.graphdb.Resource;
import org.neo4j.kernel.impl.locking.LockService;
import org.neo4j.kernel.impl.store.RecordCursors;
import org.neo4j.kernel.impl.store.record.RelationshipRecord;
import org.neo4j.kernel.impl.util.InstanceCache;

import static org.neo4j.kernel.impl.store.record.RecordLoad.CHECK;

/**
 * Cursor for iterating a set of relationships.
 */
class StoreIteratorRelationshipCursor extends StoreAbstractRelationshipCursor
{
    private LongIterator iterator;
    private final InstanceCache<StoreIteratorRelationshipCursor> instanceCache;

    StoreIteratorRelationshipCursor( RelationshipRecord relationshipRecord,
            InstanceCache<StoreIteratorRelationshipCursor> instanceCache,
            RecordCursors cursors,
            LockService lockService )
    {
        super( relationshipRecord, cursors, lockService );
        this.instanceCache = instanceCache;
    }

    public StoreIteratorRelationshipCursor init( LongIterator iterator )
    {
        this.iterator = iterator;
        return this;
    }

    @Override
    public boolean next()
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
        if ( iterator instanceof Resource )
        {
            ((Resource) iterator).close();
        }
        iterator = null;

        instanceCache.accept( this );
    }
}
