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
import java.util.function.Predicate;
import org.neo4j.function.Predicates;
import org.neo4j.graphdb.Direction;
import org.neo4j.internal.kernel.api.RelationshipDataReader;
import org.neo4j.internal.kernel.api.helpers.traversal.SlotOrName;
import org.neo4j.internal.kernel.api.helpers.traversal.productgraph.NodeJuxtaposition;
import org.neo4j.internal.kernel.api.helpers.traversal.productgraph.RelationshipExpansion;
import org.neo4j.internal.kernel.api.helpers.traversal.productgraph.State;

/** A light wrapper over productgraph.State that autogenerates the state ID and facilitates addition of new transitions */
public class PGStateBuilder {
    private int nextId = 0;
    private BuilderState startState = null;

    public BuilderState newState(boolean isStartState, boolean isFinalState) {
        return newState(null, isStartState, isFinalState);
    }

    public BuilderState newState(String name, boolean isStartState, boolean isFinalState) {
        var state = new BuilderState(new org.neo4j.internal.kernel.api.helpers.traversal.productgraph.State(
                this.nextId++,
                name == null ? SlotOrName.none() : new SlotOrName.VarName(name, false),
                new NodeJuxtaposition[] {},
                new RelationshipExpansion[] {},
                isStartState,
                isFinalState));
        if (isStartState) {
            if (startState == null) {
                startState = state;
            } else {
                throw new IllegalStateException("There is already a start state");
            }
        }
        return state;
    }

    public BuilderState getStart() {
        return this.startState;
    }

    public int stateCount() {
        return this.nextId;
    }

    public BuilderState newState() {
        return newState(false, false);
    }

    public BuilderState newStartState() {
        return newState(true, false);
    }

    public BuilderState newFinalState() {
        return newState(false, true);
    }

    public BuilderState newState(String name) {
        return newState(name, false, false);
    }

    public BuilderState newStartState(String name) {
        return newState(name, true, false);
    }

    public BuilderState newFinalState(String name) {
        return newState(name, false, true);
    }

    public static class BuilderState {
        private final org.neo4j.internal.kernel.api.helpers.traversal.productgraph.State state;

        private BuilderState(org.neo4j.internal.kernel.api.helpers.traversal.productgraph.State state) {
            this.state = state;
        }

        public void addNodeJuxtaposition(BuilderState target) {
            this.addNodeJuxtaposition(target, Predicates.ALWAYS_TRUE_LONG);
        }

        public void addNodeJuxtaposition(BuilderState target, LongPredicate nodePredicate) {
            var existing = this.state.getNodeJuxtapositions();
            var longer = new NodeJuxtaposition[existing.length + 1];
            System.arraycopy(existing, 0, longer, 0, existing.length);
            longer[existing.length] = new NodeJuxtaposition(nodePredicate, target.state);
            this.state.setNodeJuxtapositions(longer);
        }

        public void addRelationshipExpansion(BuilderState target) {
            addRelationshipExpansion(
                    target, Predicates.alwaysTrue(), null, Direction.BOTH, Predicates.ALWAYS_TRUE_LONG);
        }

        public void addRelationshipExpansion(BuilderState target, LongPredicate nodePredicate) {
            addRelationshipExpansion(target, Predicates.alwaysTrue(), null, Direction.BOTH, nodePredicate);
        }

        public void addRelationshipExpansion(
                BuilderState target, Predicate<RelationshipDataReader> relationshipPredicate) {
            addRelationshipExpansion(target, relationshipPredicate, null, Direction.BOTH, Predicates.ALWAYS_TRUE_LONG);
        }

        public void addRelationshipExpansion(BuilderState target, Direction direction) {
            addRelationshipExpansion(target, Predicates.alwaysTrue(), null, direction, Predicates.ALWAYS_TRUE_LONG);
        }

        public void addRelationshipExpansion(BuilderState target, int[] types) {
            addRelationshipExpansion(
                    target, Predicates.alwaysTrue(), types, Direction.BOTH, Predicates.ALWAYS_TRUE_LONG);
        }

        public void addRelationshipExpansion(
                BuilderState target,
                Predicate<RelationshipDataReader> relPredicate,
                int[] types,
                Direction direction,
                LongPredicate nodePredicate) {

            var existing = this.state.getRelationshipExpansions();
            var longer = new RelationshipExpansion[existing.length + 1];
            System.arraycopy(existing, 0, longer, 0, existing.length);
            longer[existing.length] = new RelationshipExpansion(
                    relPredicate, types, direction, SlotOrName.none(), nodePredicate, target.state);
            this.state.setRelationshipExpansions(longer);
        }

        public State state() {
            return this.state;
        }
    }
}
