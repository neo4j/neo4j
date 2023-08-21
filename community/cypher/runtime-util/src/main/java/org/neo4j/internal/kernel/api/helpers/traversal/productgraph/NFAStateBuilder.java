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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.function.LongPredicate;
import java.util.function.Predicate;
import org.neo4j.graphdb.Direction;
import org.neo4j.internal.kernel.api.RelationshipTraversalCursor;

public class NFAStateBuilder {
    private int id;

    private final ArrayList<BuilderState> builderStates;
    private HashMap<Integer, State> idToState;

    public NFAStateBuilder() {
        this.id = 0;
        this.builderStates = new ArrayList<>();
    }

    public BuilderState newState(State.VarName varName, boolean isStartState, boolean isFinalState) {
        BuilderState builderState = new BuilderState(this.id++, varName, isStartState, isFinalState);
        builderStates.add(builderState);
        return builderState;
    }

    public BuilderState newState() {
        return newState(new State.VarName("STATE-" + id, true), false, false);
    }

    public BuilderState newStartState() {
        return newState(new State.VarName("START-STATE-" + id, true), true, false);
    }

    public BuilderState newFinalState() {
        return newState(new State.VarName("FINAL-STATE-" + id, true), false, true);
    }

    public BuilderState newStartAndFinalState() {
        return newState(new State.VarName("START-AND-FINAL-STATE-" + id, true), true, true);
    }

    public State build() {
        State startState = null;

        HashMap<NodeJuxtaposition, Integer> nodeLinks = new HashMap<>();
        HashMap<RelationshipExpansion, Integer> relLinks = new HashMap<>();

        idToState = new HashMap<>();

        for (BuilderState builderState : builderStates) {
            var state = builderState.build(nodeLinks, relLinks);

            if (builderState.isStartState) {
                startState = state;
            }

            idToState.put(builderState.id, state);
        }

        for (var nodeLinkEntry : nodeLinks.entrySet()) {
            nodeLinkEntry.getKey().setTargetState(idToState.get(nodeLinkEntry.getValue()));
        }

        for (var relLinkEntry : relLinks.entrySet()) {
            relLinkEntry.getKey().setTargetState(idToState.get(relLinkEntry.getValue()));
        }

        assert startState != null : "There should be exactly one start state";
        return startState;
    }

    public record BuilderNodeJuxtaposition(LongPredicate nodePredicates, BuilderState targetBuilderState) {
        private NodeJuxtaposition build(Map<NodeJuxtaposition, Integer> links) {
            NodeJuxtaposition nodeJuxtaposition = new NodeJuxtaposition(nodePredicates, null);
            links.put(nodeJuxtaposition, targetBuilderState.id());
            return nodeJuxtaposition;
        }
    }

    public record BuilderRelationshipExpansion(
            Predicate<RelationshipTraversalCursor> relPredicate,
            int[] types,
            Direction direction,
            State.VarName relName,
            LongPredicate nodePredicate,
            BuilderState targetBuilderState) {
        private RelationshipExpansion build(Map<RelationshipExpansion, Integer> links) {
            RelationshipExpansion relationshipExpansion =
                    new RelationshipExpansion(relPredicate, types, direction, relName, nodePredicate, null);
            links.put(relationshipExpansion, targetBuilderState.id());
            return relationshipExpansion;
        }
    }

    public class BuilderState {
        protected final int id;
        protected final State.VarName varName;

        private final ArrayList<BuilderNodeJuxtaposition> builderNodeJuxtapositions;
        private final ArrayList<BuilderRelationshipExpansion> builderRelationshipExpansions;
        private final boolean isStartState;
        private final boolean isFinalState;

        private BuilderState(int id, State.VarName varName, boolean isStartState, boolean isFinalState) {
            this.id = id;
            this.varName = varName;
            this.isStartState = isStartState;
            this.isFinalState = isFinalState;
            builderNodeJuxtapositions = new ArrayList<>();
            builderRelationshipExpansions = new ArrayList<>();
        }

        public void addNodeJuxtaposition(BuilderNodeJuxtaposition builderNodeJuxtaposition) {
            builderNodeJuxtapositions.add(builderNodeJuxtaposition);
        }

        public void addNodeJuxtaposition(LongPredicate nodePredicates, BuilderState targetBuilderState) {
            addNodeJuxtaposition(new BuilderNodeJuxtaposition(nodePredicates, targetBuilderState));
        }

        public void addRelationshipExpansion(BuilderRelationshipExpansion builderRelationshipExpansion) {
            builderRelationshipExpansions.add(builderRelationshipExpansion);
        }

        public void addRelationshipExpansion(
                Predicate<RelationshipTraversalCursor> relPredicate,
                int[] types,
                Direction direction,
                State.VarName relName,
                LongPredicate nodePredicate,
                BuilderState targetBuilderState) {
            addRelationshipExpansion(new BuilderRelationshipExpansion(
                    relPredicate, types, direction, relName, nodePredicate, targetBuilderState));
        }

        public State build(Map<NodeJuxtaposition, Integer> nodeLinks, Map<RelationshipExpansion, Integer> relLinks) {
            return new State(
                    id,
                    varName,
                    builderNodeJuxtapositions.stream()
                            .map(t -> t.build(nodeLinks))
                            .toArray(NodeJuxtaposition[]::new),
                    builderRelationshipExpansions.stream()
                            .map(t -> t.build(relLinks))
                            .toArray(RelationshipExpansion[]::new),
                    isStartState,
                    isFinalState);
        }

        public State state() {
            assert idToState != null
                    : "A BuilderState can't be converted to a State before the "
                            + "ProductGraphTraversalCursorBuilder which built the state has had .build() called on it";
            return idToState.get(id);
        }

        public int id() {
            return id;
        }

        public State.VarName varName() {
            return varName;
        }
    }
}
