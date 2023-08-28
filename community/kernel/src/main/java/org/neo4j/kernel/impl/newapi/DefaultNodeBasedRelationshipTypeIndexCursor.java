/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
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
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */
package org.neo4j.kernel.impl.newapi;

import org.eclipse.collections.api.iterator.LongIterator;
import org.eclipse.collections.api.set.primitive.LongSet;
import org.neo4j.internal.kernel.api.NodeCursor;
import org.neo4j.internal.kernel.api.PropertyCursor;
import org.neo4j.internal.kernel.api.security.AccessMode;
import org.neo4j.internal.schema.IndexOrder;
import org.neo4j.kernel.api.index.IndexProgressor;
import org.neo4j.storageengine.api.PropertySelection;
import org.neo4j.storageengine.api.Reference;

public class DefaultNodeBasedRelationshipTypeIndexCursor
        extends IndexCursor<IndexProgressor, DefaultNodeBasedRelationshipTypeIndexCursor>
        implements InternalRelationshipTypeIndexCursor {

    private final DefaultNodeCursor nodeCursor;
    private final DefaultRelationshipTraversalCursor relationshipTraversalCursor;

    DefaultNodeBasedRelationshipTypeIndexCursor(
            CursorPool<DefaultNodeBasedRelationshipTypeIndexCursor> pool,
            DefaultNodeCursor nodeCursor,
            DefaultRelationshipTraversalCursor relationshipTraversalCursor) {
        super(pool);
        this.nodeCursor = nodeCursor;
        this.relationshipTraversalCursor = relationshipTraversalCursor;
    }

    @Override
    public void initialize(IndexProgressor progressor, int token, IndexOrder order) {}

    @Override
    public void initialize(
            IndexProgressor progressor, int token, LongIterator added, LongSet removed, AccessMode accessMode) {}

    @Override
    public boolean acceptEntity(long reference, int tokenId) {
        return false;
    }

    @Override
    public boolean isClosed() {
        return false;
    }

    @Override
    public boolean next() {
        return false;
    }

    @Override
    public void properties(PropertyCursor cursor, PropertySelection selection) {}

    @Override
    public Reference propertiesReference() {
        return null;
    }

    @Override
    public float score() {
        return 0;
    }

    @Override
    public long relationshipReference() {
        return 0;
    }

    @Override
    public int type() {
        return 0;
    }

    @Override
    public void source(NodeCursor cursor) {}

    @Override
    public void target(NodeCursor cursor) {}

    @Override
    public long sourceNodeReference() {
        return 0;
    }

    @Override
    public long targetNodeReference() {
        return 0;
    }

    @Override
    public boolean readFromStore() {
        return false;
    }

    @Override
    public void release() {}

    @Override
    public void setRead(Read read) {}
}
