/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.unsafe.batchinsert;

import java.util.Iterator;

import org.neo4j.function.Consumers;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.NotFoundException;
import org.neo4j.helpers.collection.PrefetchingIterator;
import org.neo4j.kernel.impl.api.store.StoreNodeRelationshipCursor;
import org.neo4j.kernel.impl.store.InvalidRecordException;
import org.neo4j.kernel.impl.store.NeoStores;
import org.neo4j.kernel.impl.store.record.NodeRecord;
import org.neo4j.kernel.impl.store.record.RelationshipGroupRecord;
import org.neo4j.kernel.impl.store.record.RelationshipRecord;

import static org.neo4j.kernel.impl.locking.LockService.NO_LOCK_SERVICE;

abstract class BatchRelationshipIterable<T> implements Iterable<T>
{
    private final StoreNodeRelationshipCursor relationshipCursor;

    public BatchRelationshipIterable( NeoStores neoStores, long nodeId )
    {
        RelationshipRecord relationshipRecord = new RelationshipRecord( -1 );
        RelationshipGroupRecord relationshipGroupRecord = new RelationshipGroupRecord( -1, -1 );
        this.relationshipCursor = new StoreNodeRelationshipCursor(
                relationshipRecord, neoStores,
                relationshipGroupRecord, null,
                Consumers.<StoreNodeRelationshipCursor>noop(), NO_LOCK_SERVICE );

        // TODO There's an opportunity to reuse lots of instances created here, but this isn't a
        // critical path instance so perhaps not necessary a.t.m.
        try
        {
            NodeRecord nodeRecord = neoStores.getNodeStore().getRecord( nodeId );
            relationshipCursor.init( nodeRecord.isDense(), nodeRecord.getNextRel(), nodeId,
                    Direction.BOTH );
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
