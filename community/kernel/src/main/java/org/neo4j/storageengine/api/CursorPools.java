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
package org.neo4j.storageengine.api;

import org.neo4j.cursor.Cursor;
import org.neo4j.function.Disposable;
import org.neo4j.kernel.impl.api.store.NodeDegreeCounter;
import org.neo4j.kernel.impl.locking.Lock;
import org.neo4j.storageengine.api.txstate.NodeTransactionStateView;
import org.neo4j.storageengine.api.txstate.PropertyContainerState;
import org.neo4j.storageengine.api.txstate.ReadableTransactionState;

public interface CursorPools extends Disposable
{
    /**
     * Acquires {@link Cursor} capable of {@link Cursor#get() serving} {@link NodeItem} for selected nodes.
     * No node is selected when this method returns, a call to {@link Cursor#next()} will have to be made
     * to place the cursor over the first item and then more calls to move the cursor through the selection.
     *
     * @param progression the progression of the selected nodes to be fetched
     * @param stateView the transaction state view for nodes
     * @return a {@link Cursor} over {@link NodeItem} for the given {@code nodeId}.
     */
    Cursor<NodeItem> acquireNodeCursor( BatchingLongProgression progression, NodeTransactionStateView stateView );

    /**
     * Acquires {@link Cursor} capable of {@link Cursor#get() serving} {@link RelationshipItem} for selected
     * relationships. No relationship is selected when this method returns, a call to {@link Cursor#next()}
     * will have to be made to place the cursor over the first item and then more calls to move the cursor
     * through the selection.
     *
     * @param relationshipId id of relationship to get cursor for.
     * @param state the transaction state or null if there are no changes.
     * @return a {@link Cursor} over {@link RelationshipItem} for the given {@code relationshipId}.
     */
    Cursor<RelationshipItem> acquireSingleRelationshipCursor( long relationshipId, ReadableTransactionState state );

    /**
     * Acquires {@link Cursor} capable of {@link Cursor#get() serving} {@link RelationshipItem} for selected
     * relationships. No relationship is selected when this method returns, a call to {@link Cursor#next()}
     * will have to be made to place the cursor over the first item and then more calls to move the cursor
     * through the selection.
     *
     * @param isDense if the node is dense
     * @param nodeId the id of the node where to start traversing the relationships
     * @param relationshipId the id of the first relationship in the chain
     * @param direction the direction of the relationship wrt the node
     * @param relTypes the allowed types (it allows all types if null)
     * @param state the transaction state or null if there are no changes.
     * @return a {@link Cursor} over {@link RelationshipItem} for traversing the relationships associated to the node.
     */
    Cursor<RelationshipItem> acquireNodeRelationshipCursor( boolean isDense, long nodeId, long relationshipId,
            Direction direction, int[] relTypes, ReadableTransactionState state );

    /**
     * Acquires {@link Cursor} capable of {@link Cursor#get() serving} {@link RelationshipItem} for selected
     * relationships. No relationship is selected when this method returns, a call to {@link Cursor#next()}
     * will have to be made to place the cursor over the first item and then more calls to move the cursor
     * through the selection.
     *
     * @param state the transaction state or null if there are no changes.
     * @return a {@link Cursor} over all stored relationships.
     */
    Cursor<RelationshipItem> relationshipsGetAllCursor( ReadableTransactionState state );

    Cursor<PropertyItem> acquirePropertyCursor( long propertyId, Lock shortLivedReadLock, PropertyContainerState state );

    Cursor<PropertyItem> acquireSinglePropertyCursor( long propertyId, int propertyKeyId, Lock shortLivedReadLock,
            PropertyContainerState state );

    Cursor<RelationshipGroupItem> acquireRelationshipGroupCursor( long relationshipGroupId );

    NodeDegreeCounter acquireNodeDegreeCounter( long nodeId, long relationshipGroupId );
}
