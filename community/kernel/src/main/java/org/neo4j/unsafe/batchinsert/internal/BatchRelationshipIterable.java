/*
 * Copyright (c) 2002-2019 "Neo4j,"
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
package org.neo4j.unsafe.batchinsert.internal;

import java.util.Iterator;

import org.neo4j.graphdb.NotFoundException;
import org.neo4j.helpers.collection.PrefetchingIterator;
import org.neo4j.kernel.impl.api.store.StoreNodeRelationshipCursor;
import org.neo4j.kernel.impl.store.InvalidRecordException;
import org.neo4j.kernel.impl.store.NeoStores;
import org.neo4j.kernel.impl.store.NodeStore;
import org.neo4j.kernel.impl.store.RecordCursors;
import org.neo4j.kernel.impl.store.RecordStore;
import org.neo4j.kernel.impl.store.RelationshipStore;
import org.neo4j.kernel.impl.store.record.NodeRecord;
import org.neo4j.kernel.impl.store.record.RelationshipGroupRecord;
import org.neo4j.kernel.impl.store.record.RelationshipRecord;
import org.neo4j.storageengine.api.Direction;

import static org.neo4j.function.Predicates.ALWAYS_TRUE_INT;
import static org.neo4j.kernel.impl.locking.LockService.NO_LOCK_SERVICE;
import static org.neo4j.kernel.impl.store.record.RecordLoad.NORMAL;

abstract class BatchRelationshipIterable<T> implements Iterable<T>
{
    private final StoreNodeRelationshipCursor relationshipCursor;

    BatchRelationshipIterable( NeoStores neoStores, long nodeId, RecordCursors cursors )
    {
        RelationshipStore relationshipStore = neoStores.getRelationshipStore();
        RecordStore<RelationshipGroupRecord> relationshipGroupStore = neoStores.getRelationshipGroupStore();
        RelationshipRecord relationshipRecord = relationshipStore.newRecord();
        RelationshipGroupRecord relationshipGroupRecord = relationshipGroupStore.newRecord();
        this.relationshipCursor = new StoreNodeRelationshipCursor( relationshipRecord, relationshipGroupRecord,
                cursor -> {}, cursors, NO_LOCK_SERVICE );

        // TODO There's an opportunity to reuse lots of instances created here, but this isn't a
        // critical path instance so perhaps not necessary a.t.m.
        try
        {
            NodeStore nodeStore = neoStores.getNodeStore();
            NodeRecord nodeRecord = nodeStore.getRecord( nodeId, nodeStore.newRecord(), NORMAL );
            relationshipCursor
                    .init( nodeRecord.isDense(), nodeRecord.getNextRel(), nodeId, Direction.BOTH, ALWAYS_TRUE_INT );
        }
        catch ( InvalidRecordException e )
        {
            throw new NotFoundException( "Node " + nodeId + " not found" );
        }
    }

    @Override
    public Iterator<T> iterator()
    {
        return new PrefetchingIterator<T>()
        {
            @Override
            protected T fetchNextOrNull()
            {
                if ( !relationshipCursor.next() )
                {
                    return null;
                }

                return nextFrom( relationshipCursor.id(), relationshipCursor.type(),
                        relationshipCursor.startNode(), relationshipCursor.endNode() );
            }
        };
    }

    protected abstract T nextFrom( long relId, int type, long startNode, long endNode );
}
