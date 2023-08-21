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
import static org.neo4j.collection.PrimitiveLongCollections.asArray;
import static org.neo4j.kernel.impl.api.state.RelationshipChangesForNode.DiffStrategy.ADD;
import static org.neo4j.kernel.impl.api.state.RelationshipChangesForNode.DiffStrategy.REMOVE;
import static org.neo4j.kernel.impl.api.state.RelationshipChangesForNode.createRelationshipChangesForNode;
import static org.neo4j.memory.EmptyMemoryTracker.INSTANCE;
import static org.neo4j.storageengine.api.RelationshipDirection.INCOMING;
import static org.neo4j.storageengine.api.RelationshipDirection.LOOP;
import static org.neo4j.storageengine.api.RelationshipDirection.OUTGOING;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;
import org.eclipse.collections.api.iterator.LongIterator;
import org.eclipse.collections.api.map.primitive.MutableIntObjectMap;
import org.eclipse.collections.api.set.primitive.MutableLongSet;
import org.eclipse.collections.impl.factory.primitive.IntObjectMaps;
import org.eclipse.collections.impl.factory.primitive.LongSets;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.neo4j.graphdb.Direction;
import org.neo4j.storageengine.api.RelationshipDirection;
import org.neo4j.storageengine.api.txstate.RelationshipModifications;
import org.neo4j.test.RandomSupport;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.RandomExtension;

@ExtendWith(RandomExtension.class)
class RelationshipChangesForNodeTest {
    @Inject
    private RandomSupport random;

    @Test
    void shouldGetRelationships() {
        RelationshipChangesForNode changes = createRelationshipChangesForNode(ADD, INSTANCE);

        final int TYPE = 2;

        changes.addRelationship(1, TYPE, INCOMING);
        changes.addRelationship(2, TYPE, OUTGOING);
        changes.addRelationship(3, TYPE, OUTGOING);
        changes.addRelationship(4, TYPE, LOOP);
        changes.addRelationship(5, TYPE, LOOP);
        changes.addRelationship(6, TYPE, LOOP);

        LongIterator rawRelationships = changes.getRelationships();
        assertThat(asArray(rawRelationships)).containsExactly(1, 2, 3, 4, 5, 6);
    }

    @Test
    void shouldGetRelationshipsByTypeAndDirection() {
        RelationshipChangesForNode changes = createRelationshipChangesForNode(ADD, INSTANCE);

        final int TYPE = 2;
        final int DECOY_TYPE = 666;

        changes.addRelationship(1, TYPE, INCOMING);
        changes.addRelationship(2, TYPE, OUTGOING);
        changes.addRelationship(3, TYPE, OUTGOING);
        changes.addRelationship(4, TYPE, LOOP);
        changes.addRelationship(5, TYPE, LOOP);
        changes.addRelationship(6, TYPE, LOOP);

        changes.addRelationship(10, DECOY_TYPE, INCOMING);
        changes.addRelationship(11, DECOY_TYPE, OUTGOING);
        changes.addRelationship(12, DECOY_TYPE, LOOP);
        LongIterator rawIncoming = changes.getRelationships(Direction.INCOMING, TYPE);
        assertThat(asArray(rawIncoming)).containsExactly(1, 4, 5, 6);

        LongIterator rawOutgoing = changes.getRelationships(Direction.OUTGOING, TYPE);
        assertThat(asArray(rawOutgoing)).containsExactly(2, 3, 4, 5, 6);
    }

    @Test
    void shouldVisitRelationshipIds() {
        // given
        RelationshipChangesForNode changes = createRelationshipChangesForNode(REMOVE, INSTANCE);
        MutableIntObjectMap<Map<RelationshipDirection, MutableLongSet>> expected = IntObjectMaps.mutable.empty();
        MutableLongSet allExpected = LongSets.mutable.empty();
        for (int id = 0; id < 100; id++) {
            int type = random.nextInt(5);
            RelationshipDirection direction = random.nextBoolean() ? random.nextBoolean() ? OUTGOING : INCOMING : LOOP;
            changes.addRelationship(id, type, direction);
            expected.getIfAbsentPut(type, HashMap::new)
                    .computeIfAbsent(direction, d -> LongSets.mutable.empty())
                    .add(id);
            allExpected.add(id);
        }

        // when
        MutableLongSet allChangedIds = LongSets.mutable.empty();
        changes.visitIds(allChangedIds::add);

        // then
        assertThat(allChangedIds).isEqualTo(allExpected);

        // and when
        changes.visitIdsSplit(
                typeIds -> {
                    Map<RelationshipDirection, MutableLongSet> dirMap = expected.remove(typeIds.type());
                    visitExpectedIds(typeIds, dirMap, OUTGOING, RelationshipModifications.NodeRelationshipTypeIds::out);
                    visitExpectedIds(typeIds, dirMap, INCOMING, RelationshipModifications.NodeRelationshipTypeIds::in);
                    visitExpectedIds(typeIds, dirMap, LOOP, RelationshipModifications.NodeRelationshipTypeIds::loop);
                    assertThat(dirMap).isEmpty();
                    return false;
                },
                RelationshipModifications.noAdditionalDataDecorator());
        assertThat(expected).isEmpty();
    }

    @Test
    void shouldReportHasRelationshipsOfType() {
        // given
        int type = 1;
        RelationshipChangesForNode changes = createRelationshipChangesForNode(ADD, INSTANCE);
        assertThat(changes.hasRelationships(type)).isFalse();

        long relId = 123;
        RelationshipDirection[] directions = new RelationshipDirection[] {OUTGOING, INCOMING, LOOP};
        for (int i = 0; i < directions.length; i++) {
            // when/then
            changes.addRelationship(relId + i, type, directions[i]);
            assertThat(changes.hasRelationships(type)).isTrue();
        }
        for (RelationshipDirection direction : directions) {
            // when/then
            assertThat(changes.hasRelationships(type)).isTrue();
            changes.removeRelationship(relId++, type, direction);
        }
        // and then
        assertThat(changes.hasRelationships(type)).isFalse();
    }

    private static void visitExpectedIds(
            RelationshipModifications.NodeRelationshipTypeIds typeIds,
            Map<RelationshipDirection, MutableLongSet> dirMap,
            RelationshipDirection direction,
            Function<RelationshipModifications.NodeRelationshipTypeIds, RelationshipModifications.RelationshipBatch>
                    dude) {
        if (dirMap.containsKey(direction)) {
            dude.apply(typeIds)
                    .forEach((id, type, startNode, endNode, props) ->
                            assertThat(dirMap.get(direction).remove(id)).isTrue());
            assertThat(dirMap.remove(direction).size()).isEqualTo(0);
        }
    }
}
