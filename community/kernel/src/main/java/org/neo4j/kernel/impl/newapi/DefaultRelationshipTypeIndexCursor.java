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

import org.neo4j.internal.kernel.api.KernelReadTracer;
import org.neo4j.internal.kernel.api.NodeCursor;
import org.neo4j.internal.kernel.api.RelationshipTypeIndexCursor;
import org.neo4j.internal.kernel.api.security.AccessMode;

public abstract class DefaultRelationshipTypeIndexCursor
        extends DefaultEntityTokenIndexCursor<DefaultRelationshipTypeIndexCursor>
        implements RelationshipTypeIndexCursor {

    protected DefaultRelationshipTypeIndexCursor(CursorPool<DefaultRelationshipTypeIndexCursor> pool) {
        super(pool);
    }

    @Override
    public final int type() {
        return tokenId;
    }

    @Override
    public final float score() {
        return Float.NaN;
    }

    @Override
    public long relationshipReference() {
        return entityReference();
    }

    @Override
    public void source(NodeCursor cursor) {
        read.singleNode(sourceNodeReference(), cursor);
    }

    @Override
    public void target(NodeCursor cursor) {
        read.singleNode(targetNodeReference(), cursor);
    }

    @Override
    protected final boolean allowedToSeeAllEntitiesWithToken(AccessMode accessMode, int token) {
        return accessMode.allowsTraverseRelType(token) && accessMode.allowsTraverseAllLabels();
    }

    @Override
    protected void traceNext(KernelReadTracer tracer, long entity) {
        tracer.onRelationship(entity);
    }

    @Override
    protected final void traceScan(KernelReadTracer tracer, int token) {
        tracer.onRelationshipTypeScan(token);
    }
}
