/*
 * Copyright (c) 2002-2020 "Neo4j,"
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

import org.apache.commons.lang3.exception.CloneFailedException;

import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.kernel.impl.store.RelationshipStore;
import org.neo4j.kernel.impl.store.record.RecordLoad;
import org.neo4j.kernel.impl.store.record.RelationshipRecord;
import org.neo4j.storageengine.api.RelationshipVisitor;
import org.neo4j.storageengine.api.StorageRelationshipCursor;

abstract class RecordRelationshipCursor extends RelationshipRecord implements RelationshipVisitor<RuntimeException>, StorageRelationshipCursor
{
    final RelationshipStore relationshipStore;

    RecordRelationshipCursor( RelationshipStore relationshipStore )
    {
        super( NO_ID );
        this.relationshipStore = relationshipStore;
    }

    @Override
    public long entityReference()
    {
        return getId();
    }

    @Override
    public int type()
    {
        return getType();
    }

    @Override
    public boolean hasProperties()
    {
        return nextProp != NO_ID;
    }

    @Override
    public long sourceNodeReference()
    {
        return getFirstNode();
    }

    @Override
    public long targetNodeReference()
    {
        return getSecondNode();
    }

    @Override
    public long propertiesReference()
    {
        return getNextProp();
    }

    // used to visit transaction state
    @Override
    public void visit( long relationshipId, int typeId, long startNodeId, long endNodeId )
    {
        setId( relationshipId );
        initialize( true, NO_ID, startNodeId, endNodeId, typeId, NO_ID, NO_ID, NO_ID, NO_ID, false, false );
    }

    PageCursor relationshipPage( long reference )
    {
        return relationshipStore.openPageCursorForReading( reference );
    }

    void relationship( RelationshipRecord record, long reference, PageCursor pageCursor )
    {
        // When scanning, we inspect RelationshipRecord.inUse(), so using RecordLoad.CHECK is fine
        relationshipStore.getRecordByCursor( reference, record, RecordLoad.CHECK, pageCursor );
    }

    void relationshipFull( RelationshipRecord record, long reference, PageCursor pageCursor )
    {
        // We need to load forcefully for relationship chain traversal since otherwise we cannot
        // traverse over relationship records which have been concurrently deleted
        // (flagged as inUse = false).
        // see
        //      org.neo4j.kernel.impl.store.RelationshipChainPointerChasingTest
        //      org.neo4j.kernel.impl.locking.RelationshipCreateDeleteIT
        relationshipStore.getRecordByCursor( reference, record, RecordLoad.FORCE, pageCursor );
    }

    long relationshipHighMark()
    {
        return relationshipStore.getHighestPossibleIdInUse();
    }

    @Override
    public final RelationshipRecord clone()
    {
        throw new CloneFailedException( "Record cursors are not cloneable." );
    }
}
