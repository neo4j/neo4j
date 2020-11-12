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
package org.neo4j.internal.recordstorage;

import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.io.pagecache.tracing.cursor.PageCursorTracer;
import org.neo4j.kernel.impl.store.RelationshipStore;
import org.neo4j.kernel.impl.store.record.RecordLoadOverride;
import org.neo4j.kernel.impl.store.record.RelationshipRecord;
import org.neo4j.storageengine.api.RelationshipVisitor;
import org.neo4j.storageengine.api.StoragePropertyCursor;
import org.neo4j.storageengine.api.StorageRelationshipCursor;

import static org.neo4j.kernel.impl.store.record.RecordLoad.ALWAYS;
import static org.neo4j.kernel.impl.store.record.RecordLoad.CHECK;

abstract class RecordRelationshipCursor extends RelationshipRecord implements RelationshipVisitor<RuntimeException>, StorageRelationshipCursor
{
    final RelationshipStore relationshipStore;
    RecordLoadOverride loadMode;
    private final PageCursorTracer cursorTracer;

    RecordRelationshipCursor( RelationshipStore relationshipStore, PageCursorTracer cursorTracer )
    {
        super( NO_ID );
        this.relationshipStore = relationshipStore;
        this.cursorTracer = cursorTracer;
        this.loadMode = RecordLoadOverride.none();
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

    @Override
    public void properties( StoragePropertyCursor propertyCursor )
    {
        propertyCursor.initRelationshipProperties( getNextProp() );
    }

    // used to visit transaction state
    @Override
    public void visit( long relationshipId, int typeId, long startNodeId, long endNodeId )
    {
        setId( relationshipId );
        initialize( true, NO_ID, startNodeId, endNodeId, typeId, NO_ID, NO_ID, NO_ID, NO_ID, false, false );
    }

    protected void resetState()
    {
        loadMode = RecordLoadOverride.none();
    }

    PageCursor relationshipPage( long reference )
    {
        return relationshipStore.openPageCursorForReading( reference, cursorTracer );
    }

    void relationship( RelationshipRecord record, long reference, PageCursor pageCursor )
    {
        // When scanning, we inspect RelationshipRecord.inUse(), so using RecordLoad.CHECK is fine
        relationshipStore.getRecordByCursor( reference, record, loadMode.orElse( CHECK ).lenient(), pageCursor );
    }

    void relationshipFull( RelationshipRecord record, long reference, PageCursor pageCursor )
    {
        // We need to load forcefully for relationship chain traversal since otherwise we cannot
        // traverse over relationship records which have been concurrently deleted
        // (flagged as inUse = false).
        // see
        //      org.neo4j.kernel.impl.store.RelationshipChainPointerChasingTest
        //      org.neo4j.kernel.impl.locking.RelationshipCreateDeleteIT
        relationshipStore.getRecordByCursor( reference, record, loadMode.orElse( ALWAYS ), pageCursor );
    }

    long relationshipHighMark()
    {
        return relationshipStore.getHighestPossibleIdInUse( cursorTracer );
    }

    @Override
    public void setForceLoad()
    {
        loadMode = RecordLoadOverride.FORCE;
    }

    @Override
    public RelationshipRecord copy()
    {
        throw new UnsupportedOperationException( "Record cursors are not copyable." );
    }
}
