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

public final class State {
    private final boolean isStartState;
    private final boolean isFinalState;
    private final VarName varName;
    private final int id;
    private NodeJuxtaposition[] nodeJuxtapositions;
    private RelationshipExpansion[] relationshipExpansions;

    public State(
            int id,
            VarName varName,
            NodeJuxtaposition[] nodeJuxtapositions,
            RelationshipExpansion[] relationshipExpansions,
            boolean isStartState,
            boolean isFinalState) {
        this.id = id;
        this.varName = varName;
        this.nodeJuxtapositions = nodeJuxtapositions;
        this.relationshipExpansions = relationshipExpansions;
        this.isStartState = isStartState;
        this.isFinalState = isFinalState;
    }

    public void setNodeJuxtapositions(NodeJuxtaposition[] nodeJuxtapositions) {
        this.nodeJuxtapositions = nodeJuxtapositions;
    }

    public void setRelationshipExpansions(RelationshipExpansion[] relationshipExpansions) {
        this.relationshipExpansions = relationshipExpansions;
    }

    public NodeJuxtaposition[] getNodeJuxtapositions() {
        return nodeJuxtapositions;
    }

    public RelationshipExpansion[] getRelationshipExpansions() {
        return relationshipExpansions;
    }

    public record VarName(String name, boolean isGroupVariable) {}

    public VarName varName() {
        return varName;
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
    public boolean equals(Object obj) {
        return obj instanceof State && ((State) obj).id() == id();
    }

    @Override
    public int hashCode() {
        return Integer.hashCode(id());
    }

    @Override
    public String toString() {
        return "State(id=" + id + ", varName=" + varName + ")";
    }
}
