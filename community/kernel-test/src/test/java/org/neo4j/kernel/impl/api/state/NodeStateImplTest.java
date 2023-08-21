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
package org.neo4j.kernel.impl.api.state;

import static org.assertj.core.api.Assertions.assertThat;
import static org.neo4j.collection.factory.OnHeapCollectionsFactory.INSTANCE;
import static org.neo4j.storageengine.api.RelationshipDirection.INCOMING;
import static org.neo4j.storageengine.api.RelationshipDirection.LOOP;
import static org.neo4j.storageengine.api.RelationshipDirection.OUTGOING;

import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.neo4j.memory.EmptyMemoryTracker;
import org.neo4j.storageengine.api.RelationshipDirection;
import org.neo4j.test.RandomSupport;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.RandomExtension;

@ExtendWith(RandomExtension.class)
class NodeStateImplTest {
    @Inject
    private RandomSupport random;

    @Test
    void shouldReportNoAddedRelationshipsOnNoRelationshipsAdded() {
        // given
        NodeStateImpl state = newNodeState();

        // then
        assertThat(state.hasAddedRelationships()).isFalse();
    }

    @Test
    void shouldReportAddedRelationshipsOnRelationshipsAdded() {
        // given
        NodeStateImpl state = newNodeState();

        // when
        generateRandomRelationships().forEach(r -> state.addRelationship(r.id, r.type, r.direction));

        // then
        assertThat(state.hasAddedRelationships()).isTrue();
    }

    @Test
    void shouldReportNoAddedRelationshipsOnRelationshipsFirstAddedThenRemoved() {
        // given
        NodeStateImpl state = newNodeState();

        // when
        List<AddedRelationship> relationships = generateRandomRelationships();
        relationships.forEach(r -> state.addRelationship(r.id, r.type, r.direction));

        // then
        relationships.forEach(r -> {
            assertThat(state.hasAddedRelationships()).isTrue();
            state.removeRelationship(r.id, r.type, r.direction);
        });
        assertThat(state.hasAddedRelationships()).isFalse();
    }

    private List<AddedRelationship> generateRandomRelationships() {
        RelationshipDirection[] possibleRelationshipDirections = {OUTGOING, INCOMING, LOOP};
        int count = random.nextInt(1, 10);
        List<AddedRelationship> relationships = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            relationships.add(new AddedRelationship(
                    random.nextLong(100_000_000), random.nextInt(10), random.among(possibleRelationshipDirections)));
        }
        return relationships;
    }

    private static NodeStateImpl newNodeState() {
        return NodeStateImpl.createNodeState(99, false, INSTANCE, EmptyMemoryTracker.INSTANCE);
    }

    private static class AddedRelationship {
        private final long id;
        private final int type;
        private final RelationshipDirection direction;

        private AddedRelationship(long id, int type, RelationshipDirection direction) {
            this.id = id;
            this.type = type;
            this.direction = direction;
        }
    }
}
