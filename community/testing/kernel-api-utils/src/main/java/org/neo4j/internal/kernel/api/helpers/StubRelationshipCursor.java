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
package org.neo4j.internal.kernel.api.helpers;

import java.util.Collections;
import java.util.List;
import org.neo4j.internal.kernel.api.DefaultCloseListenable;
import org.neo4j.internal.kernel.api.KernelReadTracer;
import org.neo4j.internal.kernel.api.NodeCursor;
import org.neo4j.internal.kernel.api.PropertyCursor;
import org.neo4j.internal.kernel.api.RelationshipTraversalCursor;
import org.neo4j.storageengine.api.LongReference;
import org.neo4j.storageengine.api.PropertySelection;
import org.neo4j.storageengine.api.Reference;
import org.neo4j.storageengine.api.RelationshipSelection;

public class StubRelationshipCursor extends DefaultCloseListenable implements RelationshipTraversalCursor {
    private final List<TestRelationshipChain> store;

    private int offset;
    private int chainId;
    private boolean isClosed;
    private long nodeReference;
    private RelationshipSelection selection;
    private long neighbourNodeReference;

    public StubRelationshipCursor() {
        this(Collections.emptyList());
    }

    public StubRelationshipCursor(TestRelationshipChain chain) {
        this(Collections.singletonList(chain));
    }

    public StubRelationshipCursor(List<TestRelationshipChain> store) {
        this.store = store;
        this.chainId = 0;
        this.offset = -1;
        this.isClosed = true;
    }

    void initialize(long nodeReference, RelationshipSelection selection, long neighbourNodeReference) {
        this.nodeReference = nodeReference;
        this.selection = selection;
        this.neighbourNodeReference = neighbourNodeReference;
        this.offset = -1;
        this.isClosed = true;
        this.chainId = findChain(nodeReference);
    }

    private int findChain(long nodeReference) {
        for (int i = 0; i < store.size(); i++) {
            if (store.get(i).originNodeId() == nodeReference) {
                return i;
            }
        }
        throw new IllegalArgumentException("No chain for " + nodeReference + " found");
    }

    @Override
    public long relationshipReference() {
        return store.get(chainId).get(offset).id();
    }

    @Override
    public int type() {
        return store.get(chainId).get(offset).type();
    }

    @Override
    public void source(NodeCursor cursor) {
        throw new UnsupportedOperationException("not implemented");
    }

    @Override
    public void target(NodeCursor cursor) {
        throw new UnsupportedOperationException("not implemented");
    }

    @Override
    public void properties(PropertyCursor cursor, PropertySelection selection) {
        throw new UnsupportedOperationException("not implemented");
    }

    @Override
    public long sourceNodeReference() {
        return store.get(chainId).get(offset).source();
    }

    @Override
    public long targetNodeReference() {
        return store.get(chainId).get(offset).target();
    }

    @Override
    public Reference propertiesReference() {
        return LongReference.NULL_REFERENCE;
    }

    @Override
    public void otherNode(NodeCursor cursor) {
        throw new UnsupportedOperationException("not implemented");
    }

    @Override
    public long otherNodeReference() {
        TestRelationshipChain chain = store.get(chainId);
        TestRelationshipChain.Data relationship = chain.get(offset);
        return relationship.source() == chain.originNodeId() ? relationship.target() : relationship.source();
    }

    @Override
    public long originNodeReference() {
        return store.get(chainId).originNodeId();
    }

    @Override
    public boolean next() {
        while (chainId >= 0 && chainId < store.size() && store.get(chainId).isValidOffset(offset + 1)) {
            offset++;
            TestRelationshipChain chain = store.get(chainId);
            if (!chain.isValidOffset(offset)) {
                return false;
            }
            TestRelationshipChain.Data data = chain.get(offset);
            if (selection.test(data.type(), data.relationshipDirection(nodeReference))
                    && (neighbourNodeReference == LongReference.NULL
                            || neighbourNodeReference == otherNodeReference())) {
                return true;
            }
        }
        return false;
    }

    @Override
    public void closeInternal() {
        isClosed = true;
    }

    @Override
    public boolean isClosed() {
        return isClosed;
    }

    @Override
    public void setTracer(KernelReadTracer tracer) {
        throw new UnsupportedOperationException("not implemented");
    }

    @Override
    public void removeTracer() {
        throw new UnsupportedOperationException("not implemented");
    }
}
