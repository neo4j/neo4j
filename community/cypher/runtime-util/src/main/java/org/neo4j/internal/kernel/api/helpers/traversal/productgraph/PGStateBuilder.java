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
import org.neo4j.graphdb.Direction;
import org.neo4j.internal.kernel.api.RelationshipTraversalCursor;
import org.neo4j.internal.kernel.api.helpers.traversal.SlotOrName;

/** A light wrapper over productgraph.State that autogenerates the state ID and facilitates addition of new transitions */
public class PGStateBuilder {
    private int nextId = 0;

    public BuilderState newState(boolean isStartState, boolean isFinalState) {
        return new BuilderState(new State(
                this.nextId++,
                SlotOrName.none(),
                new NodeJuxtaposition[] {},
                new RelationshipExpansion[] {},
                isStartState,
                isFinalState));
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

    public static class BuilderState {
        private final State state;

        private BuilderState(State state) {
            this.state = state;
        }

        public void addNodeJuxtaposition(LongPredicate nodePredicate, BuilderState targetBuilderState) {
            var existing = this.state.getNodeJuxtapositions();
            var longer = new NodeJuxtaposition[existing.length + 1];
            System.arraycopy(existing, 0, longer, 0, existing.length);
            longer[existing.length] = new NodeJuxtaposition(nodePredicate, targetBuilderState.state);
            this.state.setNodeJuxtapositions(longer);
        }

        public void addRelationshipExpansion(
                Predicate<RelationshipTraversalCursor> relPredicate,
                int[] types,
                Direction direction,
                LongPredicate nodePredicate,
                BuilderState targetBuilderState) {

            var existing = this.state.getRelationshipExpansions();
            var longer = new RelationshipExpansion[existing.length + 1];
            System.arraycopy(existing, 0, longer, 0, existing.length);
            longer[existing.length] = new RelationshipExpansion(
                    relPredicate, types, direction, SlotOrName.none(), nodePredicate, targetBuilderState.state);
            this.state.setRelationshipExpansions(longer);
        }

        public State state() {
            return this.state;
        }
    }
}
