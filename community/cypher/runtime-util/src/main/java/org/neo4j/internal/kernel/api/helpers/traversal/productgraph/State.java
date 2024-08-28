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

import java.util.function.LongPredicate;
import org.neo4j.internal.kernel.api.helpers.traversal.SlotOrName;
import org.neo4j.internal.kernel.api.helpers.traversal.ppbfs.TraversalDirection;

public final class State {
    private final boolean isStartState;
    private final boolean isFinalState;
    private final int id;
    private final SlotOrName slotOrName;
    private final LongPredicate predicate;
    private NodeJuxtaposition[] nodeJuxtapositions;
    private RelationshipExpansion[] relationshipExpansions;
    private NodeJuxtaposition[] reverseNodeJuxtapositions;
    private RelationshipExpansion[] reverseRelationshipExpansions;

    public State(int id, SlotOrName slotOrName, LongPredicate predicate, boolean isStartState, boolean isFinalState) {
        this.id = id;
        this.slotOrName = slotOrName;
        this.predicate = predicate;
        this.isStartState = isStartState;
        this.isFinalState = isFinalState;
        this.nodeJuxtapositions = new NodeJuxtaposition[0];
        this.relationshipExpansions = new RelationshipExpansion[0];
        this.reverseNodeJuxtapositions = new NodeJuxtaposition[0];
        this.reverseRelationshipExpansions = new RelationshipExpansion[0];
    }

    public boolean test(long nodeId) {
        return this.predicate.test(nodeId);
    }

    public void setNodeJuxtapositions(NodeJuxtaposition[] nodeJuxtapositions) {
        this.nodeJuxtapositions = nodeJuxtapositions;
    }

    public void setRelationshipExpansions(RelationshipExpansion[] relationshipExpansions) {
        this.relationshipExpansions = relationshipExpansions;
    }

    public void setReverseNodeJuxtapositions(NodeJuxtaposition[] reverseNodeJuxtapositions) {
        this.reverseNodeJuxtapositions = reverseNodeJuxtapositions;
    }

    public void setReverseRelationshipExpansions(RelationshipExpansion[] reverseRelationshipExpansions) {
        this.reverseRelationshipExpansions = reverseRelationshipExpansions;
    }

    public NodeJuxtaposition[] getNodeJuxtapositions() {
        return nodeJuxtapositions;
    }

    public RelationshipExpansion[] getRelationshipExpansions() {
        return relationshipExpansions;
    }

    public NodeJuxtaposition[] getReverseNodeJuxtapositions() {
        return reverseNodeJuxtapositions;
    }

    public RelationshipExpansion[] getReverseRelationshipExpansions() {
        return reverseRelationshipExpansions;
    }

    public NodeJuxtaposition[] getNodeJuxtapositions(TraversalDirection direction) {
        return switch (direction) {
            case FORWARD -> nodeJuxtapositions;
            case BACKWARD -> reverseNodeJuxtapositions;
        };
    }

    public RelationshipExpansion[] getRelationshipExpansions(TraversalDirection direction) {
        return switch (direction) {
            case FORWARD -> relationshipExpansions;
            case BACKWARD -> reverseRelationshipExpansions;
        };
    }

    public boolean isStartState() {
        return isStartState;
    }

    public boolean isFinalState() {
        return isFinalState;
    }

    public int id() {
        return this.id;
    }

    @Override
    public String toString() {
        return "State(id=" + id + ", slotOrName=" + slotOrName + ")";
    }

    public SlotOrName slotOrName() {
        return slotOrName;
    }
}
