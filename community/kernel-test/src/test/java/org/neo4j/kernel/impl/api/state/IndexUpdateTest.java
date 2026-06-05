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
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.memory.EmptyMemoryTracker.INSTANCE;
import static org.neo4j.storageengine.api.txstate.TransactionStateBehaviour.DEFAULT_BEHAVIOUR;

import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.neo4j.internal.schema.IndexRemovalSnapshot;
import org.neo4j.storageengine.api.txstate.TransactionStateBehaviour;
import org.neo4j.values.storable.CoordinateReferenceSystem;
import org.neo4j.values.storable.ValueTuple;
import org.neo4j.values.storable.Values;

class IndexUpdateTest {
    static final TransactionStateBehaviour WITH_INDEX_COMMANDS = new TransactionStateBehaviour() {
        @Override
        public boolean keepMetaDataForDeletedRelationship() {
            return false;
        }

        @Override
        public boolean useIndexCommands() {
            return true;
        }
    };

    private static Stream<Arguments> valueTuples() {
        return Stream.of(
                Arguments.of(ValueTuple.of(Values.intValue(1))),
                Arguments.of(ValueTuple.of(Values.stringValue("value"))),
                Arguments.of(ValueTuple.of(Values.pointValue(CoordinateReferenceSystem.CARTESIAN, 1.0, 2.0))),
                Arguments.of(ValueTuple.of(Values.byteArray(new byte[] {1, 2, 3}))),
                Arguments.of(ValueTuple.of(Values.intValue(1), Values.stringValue("str"))),
                Arguments.of(ValueTuple.of(Values.doubleValue(1.5), Values.longValue(10), Values.booleanValue(true))));
    }

    // set new prop
    @ParameterizedTest
    @MethodSource("valueTuples")
    void shouldAddNewValue(ValueTuple valueTuple) {
        var indexUpdate = IndexUpdate.createIndexUpdate(DEFAULT_BEHAVIOUR, INSTANCE);
        indexUpdate.addEntry(valueTuple, 0L);

        assertThat(indexUpdate.getAddedValueEntries().keySet()).containsExactlyInAnyOrder(valueTuple);
        assertThat(indexUpdate.getAddedValueEntries().get(valueTuple).toArray()).containsExactlyInAnyOrder(0L);
        assertThat(indexUpdate.getSortedAddedValueEntries().keySet()).containsExactly(valueTuple);
    }

    @Test
    void shouldAddManyNewValues() {
        var indexUpdate = IndexUpdate.createIndexUpdate(DEFAULT_BEHAVIOUR, INSTANCE);
        var v1 = ValueTuple.of(Values.intValue(1));
        var v2 = ValueTuple.of(Values.intValue(2));
        var v3 = ValueTuple.of(Values.intValue(3));
        indexUpdate.addEntry(v1, 0L);
        indexUpdate.addEntry(v2, 1L);
        indexUpdate.addEntry(v3, 4L);
        indexUpdate.addEntry(v2, 5L);
        indexUpdate.addEntry(v3, 2L);
        indexUpdate.addEntry(v1, 3L);

        assertThat(indexUpdate.getAddedValueEntries().keySet()).containsExactlyInAnyOrder(v3, v1, v2);
        assertThat(indexUpdate.getAddedValueEntries().get(v1).toArray()).containsExactlyInAnyOrder(0L, 3L);
        assertThat(indexUpdate.getAddedValueEntries().get(v2).toArray()).containsExactlyInAnyOrder(1L, 5L);
        assertThat(indexUpdate.getAddedValueEntries().get(v3).toArray()).containsExactlyInAnyOrder(2L, 4L);
        assertThat(indexUpdate.getSortedAddedValueEntries().keySet()).containsExactly(v1, v2, v3);
        assertThat(indexUpdate.getRemovedEntityIds().toArray()).isEmpty();
        IndexRemovalSnapshot removed = indexUpdate.removedSnapshot();
        assertTrue(removed.isEmpty());
        assertThat(removed.version()).isEqualTo(0);
    }

    // remove prop
    @Test
    void shouldRemoveValue() {
        var indexUpdate = IndexUpdate.createIndexUpdate(DEFAULT_BEHAVIOUR, INSTANCE);
        indexUpdate.removeEntry(null, 0L);

        assertThat(indexUpdate.getAddedValueEntries()).isEmpty();
        assertThat(indexUpdate.getSortedAddedValueEntries()).isEmpty();
        assertThat(indexUpdate.getRemovedEntityIds().toArray()).containsExactlyInAnyOrder(0L);
    }

    @Test
    void shouldRemoveManyNewValues() {
        var indexUpdate = IndexUpdate.createIndexUpdate(DEFAULT_BEHAVIOUR, INSTANCE);
        indexUpdate.removeEntry(null, 0L);
        indexUpdate.removeEntry(null, 1L);
        indexUpdate.removeEntry(null, 2L);

        assertThat(indexUpdate.getAddedValueEntries()).isEmpty();
        assertThat(indexUpdate.getSortedAddedValueEntries()).isEmpty();
        assertThat(indexUpdate.getRemovedEntityIds().toArray()).containsExactlyInAnyOrder(0L, 1L, 2L);
    }

    @Test
    void shouldThrowOnGetRemovedValueWithoutIndexCommands() {
        var indexUpdate = IndexUpdate.createIndexUpdate(DEFAULT_BEHAVIOUR, INSTANCE);
        indexUpdate.removeEntry(null, 0L);

        assertThatThrownBy(indexUpdate::getRemovedValueEntries).isInstanceOf(IllegalArgumentException.class);
    }

    // remove prop with index commands
    @ParameterizedTest
    @MethodSource("valueTuples")
    void shouldRemoveValueWithIndexCommands(ValueTuple valueTuple) {
        var indexUpdate = IndexUpdate.createIndexUpdate(WITH_INDEX_COMMANDS, INSTANCE);
        indexUpdate.removeEntry(valueTuple, 0L);

        assertThat(indexUpdate.getAddedValueEntries()).isEmpty();
        assertThat(indexUpdate.getSortedAddedValueEntries()).isEmpty();
        assertThat(indexUpdate.getRemovedValueEntries().values())
                .containsExactlyInAnyOrder(new IndexUpdate.ValueTupleWithVersionImpl(valueTuple, 0));
        assertThat(indexUpdate.getRemovedValueEntries().keySet().toArray()).containsExactlyInAnyOrder(0L);
    }

    @ParameterizedTest
    @MethodSource("valueTuples")
    void shouldRemoveManyNewValuesWithIndexCommands(ValueTuple valueTuple) {
        var indexUpdate = IndexUpdate.createIndexUpdate(WITH_INDEX_COMMANDS, INSTANCE);
        ValueTuple otherValue = ValueTuple.of(Values.stringValue("other"));
        indexUpdate.removeEntry(valueTuple, 0L);
        indexUpdate.removeEntry(otherValue, 1L);
        indexUpdate.removeEntry(valueTuple, 2L);

        assertThat(indexUpdate.getAddedValueEntries()).isEmpty();
        assertThat(indexUpdate.getSortedAddedValueEntries()).isEmpty();
        assertThat(indexUpdate.getRemovedEntityIds().toArray()).containsExactlyInAnyOrder(0L, 1L, 2L);
        assertThat(indexUpdate.getRemovedValueEntries().keySet().toArray()).containsExactlyInAnyOrder(0L, 1L, 2L);
        assertThat(indexUpdate.getRemovedValueEntries().values())
                .containsExactlyInAnyOrder(
                        new IndexUpdate.ValueTupleWithVersionImpl(valueTuple, 0),
                        new IndexUpdate.ValueTupleWithVersionImpl(valueTuple, 0),
                        new IndexUpdate.ValueTupleWithVersionImpl(otherValue, 0));
    }

    // set new prop, remove that
    @ParameterizedTest
    @MethodSource("valueTuples")
    void shouldRemoveNewAddedValue(ValueTuple valueTuple) {
        var indexUpdate = IndexUpdate.createIndexUpdate(DEFAULT_BEHAVIOUR, INSTANCE);
        indexUpdate.addEntry(valueTuple, 0L);
        indexUpdate.removeEntry(null, 0L);

        assertThat(indexUpdate.getAddedValueEntries().get(valueTuple).toArray()).isEmpty();
        assertThat(indexUpdate.getSortedAddedValueEntries().get(valueTuple).toArray())
                .isEmpty();
        assertThat(indexUpdate.getRemovedEntityIds().toArray()).containsExactlyInAnyOrder(0L);
    }

    // set then remove existing prop without index commands should only keep last change
    @ParameterizedTest
    @MethodSource("valueTuples")
    void shouldChangeThenRemoveExistingValue(ValueTuple valueTuple) {
        var indexUpdate = IndexUpdate.createIndexUpdate(DEFAULT_BEHAVIOUR, INSTANCE);
        // change existing
        indexUpdate.removeEntry(null, 0L);
        indexUpdate.addEntry(valueTuple, 0L);
        // remove
        indexUpdate.removeEntry(null, 0L);

        assertThat(indexUpdate.getAddedValueEntries().get(valueTuple).toArray()).isEmpty();
        assertThat(indexUpdate.getSortedAddedValueEntries().get(valueTuple).toArray())
                .isEmpty();
        assertThat(indexUpdate.getRemovedEntityIds().toArray()).containsExactlyInAnyOrder(0L);
    }

    // set then remove existing prop with index commands should only have remove of first value
    @ParameterizedTest
    @MethodSource("valueTuples")
    void shouldChangeThenRemoveExistingValueWithIndexCommands(ValueTuple valueTuple) {
        var indexUpdate = IndexUpdate.createIndexUpdate(WITH_INDEX_COMMANDS, INSTANCE);
        // change existing
        ValueTuple existingValue = ValueTuple.of(Values.stringValue("existing"));
        indexUpdate.removeEntry(existingValue, 0L);
        indexUpdate.addEntry(valueTuple, 0L);
        // remove
        indexUpdate.removeEntry(valueTuple, 0L);

        assertThat(indexUpdate.getAddedValueEntries().get(valueTuple).toArray()).isEmpty();
        assertThat(indexUpdate.getSortedAddedValueEntries().get(valueTuple).toArray())
                .isEmpty();
        assertThat(indexUpdate.getRemovedValueEntries().values())
                .containsExactlyInAnyOrder(new IndexUpdate.ValueTupleWithVersionImpl(existingValue, 0));
    }

    // set existing twice should have last add and marked as removed without index commands
    @ParameterizedTest
    @MethodSource("valueTuples")
    void shouldChangeExistingValueTwice(ValueTuple valueTuple) {
        var indexUpdate = IndexUpdate.createIndexUpdate(DEFAULT_BEHAVIOUR, INSTANCE);
        // change existing
        indexUpdate.removeEntry(null, 0L);
        indexUpdate.addEntry(valueTuple, 0L);
        // change again
        ValueTuple newValue = ValueTuple.of(Values.stringValue("new"));
        indexUpdate.removeEntry(null, 0L);
        indexUpdate.addEntry(newValue, 0L);

        assertThat(indexUpdate.getAddedValueEntries().keySet()).containsExactlyInAnyOrder(newValue, valueTuple);
        assertThat(indexUpdate.getAddedValueEntries().get(valueTuple).toArray()).isEmpty();
        assertThat(indexUpdate.getAddedValueEntries().get(newValue).toArray()).containsExactlyInAnyOrder(0L);
        assertThat(indexUpdate.getRemovedEntityIds().toArray()).containsExactlyInAnyOrder(0L);
    }

    // set existing twice should have last add and first remove with index commands
    @ParameterizedTest
    @MethodSource("valueTuples")
    void shouldChangeExistingValueTwiceWithIndexCommands(ValueTuple valueTuple) {
        var indexUpdate = IndexUpdate.createIndexUpdate(WITH_INDEX_COMMANDS, INSTANCE);
        // change existing
        ValueTuple existingValue = ValueTuple.of(Values.stringValue("existing"));
        indexUpdate.removeEntry(existingValue, 0L);
        indexUpdate.addEntry(valueTuple, 0L);
        // change again
        ValueTuple newValue = ValueTuple.of(Values.stringValue("new"));
        indexUpdate.removeEntry(valueTuple, 0L);
        indexUpdate.addEntry(newValue, 0L);

        assertThat(indexUpdate.getAddedValueEntries().keySet()).containsExactlyInAnyOrder(newValue, valueTuple);
        assertThat(indexUpdate.getAddedValueEntries().get(valueTuple).toArray()).isEmpty();
        assertThat(indexUpdate.getAddedValueEntries().get(newValue).toArray()).containsExactlyInAnyOrder(0L);
        assertThat(indexUpdate.getRemovedValueEntries().values())
                .containsExactlyInAnyOrder(new IndexUpdate.ValueTupleWithVersionImpl(existingValue, 0));
    }

    // removal snapshot should see correct values, without index commands we always have all removed
    @ParameterizedTest
    @MethodSource("valueTuples")
    void shouldShowCorrectRemovalsInSnapshots(ValueTuple valueTuple) {
        var indexUpdate = IndexUpdate.createIndexUpdate(DEFAULT_BEHAVIOUR, INSTANCE);
        // make changes
        indexUpdate.removeEntry(null, 0L);
        indexUpdate.removeEntry(null, 1L);
        IndexRemovalSnapshot rs1 = indexUpdate.removedSnapshot();
        indexUpdate.addEntry(valueTuple, 1L);
        IndexRemovalSnapshot rs2 = indexUpdate.removedSnapshot();
        indexUpdate.removeEntry(null, 1L);
        indexUpdate.removeEntry(null, 2L);
        IndexRemovalSnapshot rs3 = indexUpdate.removedSnapshot();

        assertThat(indexUpdate.getRemovedEntityIds().toArray()).containsExactlyInAnyOrder(0L, 1L, 2L);
        assertThat(rs1.version()).isEqualTo(0);
        assertTrue(rs1.isRemoved().test(0L));
        assertTrue(rs1.isRemoved().test(1L));
        assertFalse(rs1.isRemoved().test(2L));

        assertThat(rs2.version()).isEqualTo(1);
        assertTrue(rs2.isRemoved().test(0L));
        assertTrue(rs2.isRemoved().test(1L));
        assertFalse(rs2.isRemoved().test(2L));

        assertThat(rs3.version()).isEqualTo(2);
        assertTrue(rs3.isRemoved().test(0L));
        assertTrue(rs3.isRemoved().test(1L));
        assertTrue(rs3.isRemoved().test(2L));
    }

    // removal snapshot should see correct values with index commands
    @ParameterizedTest
    @MethodSource("valueTuples")
    void shouldShowCorrectRemovalsInSnapshotsWithIndexCommands(ValueTuple valueTuple) {
        var indexUpdate = IndexUpdate.createIndexUpdate(WITH_INDEX_COMMANDS, INSTANCE);
        // make changes
        ValueTuple existingValue = ValueTuple.of(Values.stringValue("existing"));
        ValueTuple newValue = ValueTuple.of(Values.stringValue("new"));
        indexUpdate.removeEntry(existingValue, 0L);
        indexUpdate.removeEntry(existingValue, 1L);
        IndexRemovalSnapshot rs1 = indexUpdate.removedSnapshot();
        indexUpdate.addEntry(newValue, 1L);
        IndexRemovalSnapshot rs2 = indexUpdate.removedSnapshot();
        indexUpdate.removeEntry(newValue, 1L);
        indexUpdate.addEntry(existingValue, 1L);
        IndexRemovalSnapshot rs3 = indexUpdate.removedSnapshot();
        indexUpdate.removeEntry(existingValue, 1L);
        indexUpdate.removeEntry(existingValue, 2L);
        IndexRemovalSnapshot rs4 = indexUpdate.removedSnapshot();

        assertThat(indexUpdate.getRemovedEntityIds().toArray()).containsExactlyInAnyOrder(0L, 1L, 2L);
        // entity 0 and 1 have removed existing value
        assertThat(rs1.version()).isEqualTo(0);
        assertTrue(rs1.isRemoved().test(0L));
        assertTrue(rs1.isRemoved().test(1L));
        assertFalse(rs1.isRemoved().test(2L));

        // entity 0 and 1 have removed existing value, 1 is added with a new value, the existing should be removed
        assertThat(rs2.version()).isEqualTo(1);
        assertTrue(rs2.isRemoved().test(0L));
        assertTrue(rs2.isRemoved().test(1L));
        assertFalse(rs2.isRemoved().test(2L));

        // entity 0 has removed existing value, 1 new value removed and existing re-added so should not be removed
        assertThat(rs3.version()).isEqualTo(2);
        assertTrue(rs3.isRemoved().test(0L));
        assertFalse(rs3.isRemoved().test(1L));
        assertFalse(rs3.isRemoved().test(2L));

        // entity 0, 1 and 2 have removed existing value
        assertThat(rs4.version()).isEqualTo(3);
        assertTrue(rs4.isRemoved().test(0L));
        assertTrue(rs4.isRemoved().test(1L));
        assertTrue(rs4.isRemoved().test(2L));
    }
}
