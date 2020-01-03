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
package org.neo4j.kernel.impl.transaction.state.storeview;

import java.util.function.IntPredicate;

import org.neo4j.helpers.collection.Visitor;
import org.neo4j.kernel.api.labelscan.NodeLabelUpdate;
import org.neo4j.kernel.impl.api.index.EntityUpdates;
import org.neo4j.kernel.impl.locking.LockService;
import org.neo4j.storageengine.api.StorageNodeCursor;
import org.neo4j.storageengine.api.StorageReader;

import static org.neo4j.collection.PrimitiveLongCollections.EMPTY_LONG_ARRAY;
import static org.neo4j.kernel.api.labelscan.NodeLabelUpdate.labelChanges;

public class StoreViewNodeStoreScan<FAILURE extends Exception> extends PropertyAwareEntityStoreScan<StorageNodeCursor,FAILURE>
{
    private final Visitor<NodeLabelUpdate,FAILURE> labelUpdateVisitor;
    private final Visitor<EntityUpdates,FAILURE> propertyUpdatesVisitor;
    protected final int[] labelIds;

    public StoreViewNodeStoreScan( StorageReader storageReader, LockService locks,
            Visitor<NodeLabelUpdate,FAILURE> labelUpdateVisitor,
            Visitor<EntityUpdates,FAILURE> propertyUpdatesVisitor,
            int[] labelIds, IntPredicate propertyKeyIdFilter )
    {
        super( storageReader, storageReader.nodesGetCount(), propertyKeyIdFilter, id -> locks.acquireNodeLock( id, LockService.LockType.READ_LOCK ) );
        this.labelUpdateVisitor = labelUpdateVisitor;
        this.propertyUpdatesVisitor = propertyUpdatesVisitor;
        this.labelIds = labelIds;
    }

    @Override
    protected StorageNodeCursor allocateCursor( StorageReader storageReader )
    {
        return storageReader.allocateNodeCursor();
    }

    @Override
    public boolean process( StorageNodeCursor cursor ) throws FAILURE
    {
        long[] labels = cursor.labels();
        if ( labels.length == 0 && labelIds.length != 0 )
        {
            // This node has no labels at all
            return false;
        }

        if ( labelUpdateVisitor != null )
        {
            // Notify the label update visitor
            labelUpdateVisitor.visit( labelChanges( cursor.entityReference(), EMPTY_LONG_ARRAY, labels ) );
        }

        if ( propertyUpdatesVisitor != null && containsAnyEntityToken( labelIds, labels ) )
        {
            // Notify the property update visitor
            EntityUpdates.Builder updates = EntityUpdates.forEntity( cursor.entityReference(), true ).withTokens( labels );

            if ( hasRelevantProperty( cursor, updates ) )
            {
                return propertyUpdatesVisitor.visit( updates.build() );
            }
        }
        return false;
    }
}
