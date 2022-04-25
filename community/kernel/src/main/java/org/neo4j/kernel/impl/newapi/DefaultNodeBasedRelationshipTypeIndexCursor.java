/*
 * Copyright (c) "Neo4j"
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
package org.neo4j.kernel.impl.newapi;

import org.neo4j.graphdb.Direction;
import org.neo4j.internal.kernel.api.NodeCursor;
import org.neo4j.internal.kernel.api.PropertyCursor;
import org.neo4j.internal.kernel.api.security.AccessMode;
import org.neo4j.storageengine.api.PropertySelection;
import org.neo4j.storageengine.api.Reference;
import org.neo4j.storageengine.api.RelationshipSelection;
import org.neo4j.storageengine.api.StorageEngineIndexingBehaviour;

/**
 * {@link DefaultRelationshipTypeIndexCursor} which is node-based, i.e. the IDs driving the cursor are node IDs that contain
 * relationships of types we're interested in. For each node ID that we get from the underlying lookup index use the node cursor
 * to go there and read the relationships of the given type and iterate over those, then go to the next node ID from the lookup index, a.s.o.
 * @see StorageEngineIndexingBehaviour
 */
public class DefaultNodeBasedRelationshipTypeIndexCursor extends DefaultRelationshipTypeIndexCursor {
    private final DefaultNodeCursor nodeCursor;
    private final DefaultRelationshipTraversalCursor relationshipTraversalCursor;

    DefaultNodeBasedRelationshipTypeIndexCursor(
            CursorPool<DefaultRelationshipTypeIndexCursor> pool,
            DefaultNodeCursor nodeCursor,
            DefaultRelationshipTraversalCursor relationshipTraversalCursor) {
        super(pool);
        this.nodeCursor = nodeCursor;
        this.relationshipTraversalCursor = relationshipTraversalCursor;
    }

    @Override
    boolean allowedToSeeEntity(AccessMode accessMode, long entityReference) {
        // Security is managed by the internal node/relatiosnhipTraversal cursors, so we don't need to do any additional
        // checks here.
        return true;
    }

    @Override
    protected boolean innerNext() {
        do {
            if (relationshipTraversalCursor.next()) {
                entity = relationshipTraversalCursor.relationshipReference();
                return true;
            }

            // super.innerNext() will go to the next ID in the token scan index and eventually hit acceptEntity() on
            // this instance
            if (!super.innerNext()) {
                return false;
            }

            nodeCursor.single(entity, read);
            if (nodeCursor.next()) {
                nodeCursor.relationships(
                        relationshipTraversalCursor, RelationshipSelection.selection(tokenId, Direction.OUTGOING));
            }
        } while (true);
    }

    @Override
    public void source(NodeCursor cursor) {
        read.singleNode(relationshipTraversalCursor.sourceNodeReference(), cursor);
    }

    @Override
    public void target(NodeCursor cursor) {
        read.singleNode(relationshipTraversalCursor.targetNodeReference(), cursor);
    }

    @Override
    public long sourceNodeReference() {
        return relationshipTraversalCursor.sourceNodeReference();
    }

    @Override
    public long targetNodeReference() {
        return relationshipTraversalCursor.targetNodeReference();
    }

    @Override
    public long relationshipReference() {
        return entityReference();
    }

    @Override
    public void properties(PropertyCursor cursor, PropertySelection selection) {
        relationshipTraversalCursor.properties(cursor, selection);
    }

    @Override
    public Reference propertiesReference() {
        return relationshipTraversalCursor.propertiesReference();
    }

    @Override
    public void closeInternal() {
        if (!isClosed()) {
            nodeCursor.close();
            relationshipTraversalCursor.close();
        }
        super.closeInternal();
    }

    @Override
    public void release() {
        nodeCursor.release();
        relationshipTraversalCursor.release();
    }
}
