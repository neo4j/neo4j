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
package org.neo4j.internal.kernel.api.helpers.traversal.productgraph;

import static org.neo4j.kernel.api.StatementConstants.NO_SUCH_NODE;

import java.util.List;
import org.apache.commons.lang3.ArrayUtils;
import org.neo4j.internal.kernel.api.NodeCursor;
import org.neo4j.internal.kernel.api.RelationshipTraversalCursor;
import org.neo4j.internal.kernel.api.helpers.RelationshipSelections;
import org.neo4j.memory.MemoryTracker;
import org.neo4j.storageengine.api.DirectedTypes;
import org.neo4j.storageengine.api.RelationshipDirection;

public class ProductGraphTraversalCursor implements AutoCloseable {

    private long originNodeId = NO_SUCH_NODE;
    private boolean initialized = false;
    private final DirectedTypes directedTypes;
    private RelationshipTraversalCursor traversalCursor;
    private final SourceCursor<List<State>, RelationshipExpansion> nfaCursor;

    public ProductGraphTraversalCursor(RelationshipTraversalCursor relCursor, MemoryTracker memoryTracker) {
        this.traversalCursor = relCursor;
        this.nfaCursor = new ComposedSourceCursor<>(new ListCursor<>(), new RelationshipExpansionCursor());
        this.directedTypes = new DirectedTypes(memoryTracker);
    }

    public State targetState() {
        return nfaCursor.current().targetState();
    }

    public long otherNode() {
        return traversalCursor.otherNodeReference();
    }

    public long relationshipId() {
        return traversalCursor.reference();
    }

    public boolean next() {
        if (!initialized) {
            if (!nextRelationship()) {
                return false;
            }
            initialized = true;
        }

        while (true) {
            while (nfaCursor.next()) {
                if (evaluateCurrent()) {
                    return true;
                }
            }

            if (!nextRelationship()) {
                return false;
            }
        }
    }

    private boolean nextRelationship() {
        nfaCursor.reset();
        return traversalCursor.next();
    }

    private boolean evaluateCurrent() {
        var expansion = nfaCursor.current();
        var currentDirection = RelationshipDirection.directionOfStrict(
                originNodeId, traversalCursor.sourceNodeReference(), traversalCursor.targetNodeReference());
        return currentDirection.matches(expansion.direction())
                && (expansion.types() == null || ArrayUtils.contains(expansion.types(), traversalCursor.type()))
                && expansion.testRelationship(traversalCursor)
                && expansion.testNode(traversalCursor.otherNodeReference());
    }

    public void setNodeAndStates(NodeCursor node, List<State> states) {
        initialized = false;
        this.originNodeId = node.nodeReference();
        this.nfaCursor.setSource(states);

        // preprocess nfa type directions for the current node for use in the graph cursor
        directedTypes.clear();
        while (this.nfaCursor.next()) {
            var expansion = this.nfaCursor.current();
            directedTypes.addTypes(expansion.types(), expansion.direction());
        }
        this.nfaCursor.reset();

        RelationshipSelections.multiTypeMultiDirectionCursor(traversalCursor, node, directedTypes);
    }

    @Override
    public void close() throws Exception {
        if (traversalCursor != null) {
            traversalCursor.close();
            traversalCursor = null;
        }
        this.nfaCursor.close();
    }
}
