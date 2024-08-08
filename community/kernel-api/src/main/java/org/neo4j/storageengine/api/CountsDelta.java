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
package org.neo4j.storageengine.api;

import static org.neo4j.token.api.TokenConstants.ANY_LABEL;
import static org.neo4j.token.api.TokenConstants.ANY_RELATIONSHIP_TYPE;

import org.apache.commons.lang3.mutable.MutableLong;
import org.eclipse.collections.api.map.MutableMap;
import org.eclipse.collections.impl.map.mutable.UnifiedMap;
import org.eclipse.collections.impl.map.mutable.primitive.IntLongHashMap;

/**
 * An in-memory single-threaded counts holder useful for modifying and reading counts transaction state.
 */
public class CountsDelta {
    private static final long DEFAULT_COUNT = 0;
    protected final IntLongHashMap nodeCounts = new IntLongHashMap();
    protected final MutableMap<RelationshipKey, MutableLong> relationshipCounts = UnifiedMap.newMap();

    public long nodeCount(int labelId) {
        return nodeCounts.getIfAbsent(labelId, DEFAULT_COUNT);
    }

    public void incrementNodeCount(int labelId, long delta) {
        if (delta != 0) {
            internalIncrementNodeCount(labelId, delta);
        }
    }

    public void incrementNodeCount(int[] labelIds, long delta) {
        if (delta != 0) {
            for (int labelId : labelIds) {
                internalIncrementNodeCount(labelId, delta);
            }
        }
    }

    private void internalIncrementNodeCount(int labelId, long delta) {
        nodeCounts.updateValue(labelId, DEFAULT_COUNT, l -> l + delta);
    }

    public long relationshipCount(int startLabelId, int typeId, int endLabelId) {
        RelationshipKey relationshipKey = new RelationshipKey(startLabelId, typeId, endLabelId);
        MutableLong counts = relationshipCounts.get(relationshipKey);
        return counts == null ? 0 : counts.longValue();
    }

    public void incrementRelationshipCount(int startLabelId, int typeId, int endLabelId, long delta) {
        if (delta != 0) {
            RelationshipKey relationshipKey = new RelationshipKey(startLabelId, typeId, endLabelId);
            relationshipCounts
                    .getIfAbsentPutWithKey(relationshipKey, k -> new MutableLong(DEFAULT_COUNT))
                    .add(delta);
        }
    }

    /**
     * Increments relationship counts for when adding/removing a label on a node.
     * When adding a label the outgoing/incoming counts should be positive, otherwise negative.
     */
    public void incrementRelationshipCountsForLabelChange(int typeId, int labelId, long outgoing, long incoming) {
        // untyped
        incrementRelationshipCount(labelId, ANY_RELATIONSHIP_TYPE, ANY_LABEL, outgoing);
        incrementRelationshipCount(ANY_LABEL, ANY_RELATIONSHIP_TYPE, labelId, incoming);
        // typed
        incrementRelationshipCount(labelId, typeId, ANY_LABEL, outgoing);
        incrementRelationshipCount(ANY_LABEL, typeId, labelId, incoming);
    }

    public void accept(Visitor visitor) {
        nodeCounts.forEachKeyValue(visitor::visitNodeCount);
        relationshipCounts.forEachKeyValue((k, count) ->
                visitor.visitRelationshipCount(k.startLabelId, k.typeId, k.endLabelId, count.longValue()));
    }

    public boolean hasChanges() {
        return !nodeCounts.isEmpty() || !relationshipCounts.isEmpty();
    }

    public int size() {
        return nodeCounts.size() + relationshipCounts.size();
    }

    public void clear() {
        nodeCounts.clear();
        relationshipCounts.clear();
    }

    public record RelationshipKey(int startLabelId, int typeId, int endLabelId) {}

    public interface Visitor {
        void visitNodeCount(int labelId, long count);

        void visitRelationshipCount(int startLabelId, int typeId, int endLabelId, long count);
    }
}
