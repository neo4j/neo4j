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
package org.neo4j.internal.recordstorage;

import static java.util.Collections.emptyList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.neo4j.io.pagecache.context.CursorContext.NULL_CONTEXT;
import static org.neo4j.kernel.impl.store.record.Record.NO_LABELS_FIELD;
import static org.neo4j.memory.EmptyMemoryTracker.INSTANCE;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.extension.ExtendWith;
import org.neo4j.io.pagecache.context.CursorContext;
import org.neo4j.kernel.impl.store.DynamicNodeLabels;
import org.neo4j.kernel.impl.store.DynamicRecordAllocator;
import org.neo4j.kernel.impl.store.NodeLabelsField;
import org.neo4j.kernel.impl.store.PropertyStore;
import org.neo4j.kernel.impl.store.record.DynamicRecord;
import org.neo4j.kernel.impl.store.record.NodeRecord;
import org.neo4j.kernel.impl.store.record.PropertyBlock;
import org.neo4j.kernel.impl.store.record.PropertyRecord;
import org.neo4j.kernel.impl.store.record.RelationshipRecord;
import org.neo4j.kernel.impl.transaction.log.InMemoryClosableChannel;
import org.neo4j.test.extension.RandomExtension;
import org.neo4j.values.storable.Values;

@ExtendWith(RandomExtension.class)
public class LogCommandSerializationV5_11Test extends LogCommandSerializationV5_8Test {

    @Override
    LogCommandSerializationV5_11 createReader() {
        return LogCommandSerializationV5_11.INSTANCE;
    }

    @Override
    LogCommandSerializationV5_11 writer() {
        return LogCommandSerializationV5_11.INSTANCE;
    }

    @RepeatedTest(100)
    void readCreateNodeCommand() throws IOException {
        try (var channel = new InMemoryClosableChannel()) {
            var commandSerialization = new LogCommandSerializationV5_11();
            NodeRecord randomCreated = createRandomNodeRecord(7);
            randomCreated.setCreated();
            randomCreated.setInUse(true);

            var nodeCommand = new Command.NodeCommand(commandSerialization, randomCreated, randomCreated);
            commandSerialization.writeCreatedNodeCommand(channel, nodeCommand);

            var reader = createReader();
            var command = reader.read(channel);

            assertThat(command).isInstanceOf(Command.NodeCommand.class);
            assertThat(randomCreated).isEqualTo(((Command.NodeCommand) command).getAfter());
        }
    }

    @RepeatedTest(100)
    void readDeleteNodeCommand() throws IOException {
        try (var channel = new InMemoryClosableChannel()) {
            var commandSerialization = new LogCommandSerializationV5_11();
            NodeRecord randomRemovedNode = createRandomNodeRecord(7);
            NodeRecord nodeBefore = new NodeRecord(randomRemovedNode);

            randomRemovedNode.setInUse(false);
            randomRemovedNode.setCreated(false);

            nodeBefore.setCreated(false);
            nodeBefore.setInUse(true);

            var nodeCommand = new Command.NodeCommand(commandSerialization, nodeBefore, randomRemovedNode);
            commandSerialization.writeDeletedNodeCommand(channel, nodeCommand);

            var reader = createReader();
            var command = reader.read(channel);

            assertThat(command).isInstanceOf(Command.NodeCommand.class);
            assertThat(nodeBefore).isEqualTo(((Command.NodeCommand) command).getBefore());
        }
    }

    @RepeatedTest(100)
    void createNodesAreTheSameInNewAndOldCommands() throws IOException {
        try (var channel = new InMemoryClosableChannel()) {
            var commandSerialization = new LogCommandSerializationV5_11();
            NodeRecord recordBeforeToDelete = createRandomNodeRecord(42);
            NodeRecord recordAfterDelete = new NodeRecord(recordBeforeToDelete);

            recordBeforeToDelete.setInUse(false);
            recordBeforeToDelete.setCreated(false);

            recordAfterDelete.setCreated(true);
            recordAfterDelete.setInUse(true);

            var createNodeCommand =
                    new Command.NodeCommand(commandSerialization, recordBeforeToDelete, recordAfterDelete);

            commandSerialization.writeNodeCommand(channel, createNodeCommand);
            commandSerialization.writeCreatedNodeCommand(channel, createNodeCommand);

            var reader = createReader();
            var oldFullNodeCommand = (Command.NodeCommand) reader.read(channel);
            var newCreateNodeCommand = (Command.NodeCommand) reader.read(channel);

            assertEquals(oldFullNodeCommand.getBefore(), newCreateNodeCommand.getBefore());
            assertEquals(oldFullNodeCommand.getAfter(), newCreateNodeCommand.getAfter());
        }
    }

    @RepeatedTest(100)
    void deleteNodesAreTheSameInNewAndOldCommands() throws IOException {
        try (var channel = new InMemoryClosableChannel()) {
            var commandSerialization = new LogCommandSerializationV5_11();
            NodeRecord recordBeforeToDelete = createRandomUsedNodeRecord(42);
            NodeRecord recordAfterDelete = new NodeRecord(42);

            recordAfterDelete.setInUse(false);

            var labelRecords = recordBeforeToDelete.getDynamicLabelRecords();
            var dynamicLabelRecords = new ArrayList<DynamicRecord>(labelRecords.size());
            for (DynamicRecord labelRecord : labelRecords) {
                DynamicRecord dynamicRecord = new DynamicRecord(labelRecord);
                dynamicRecord.setInUse(false);
                dynamicLabelRecords.add(dynamicRecord);
            }
            recordAfterDelete.setLabelField(NO_LABELS_FIELD.longValue(), dynamicLabelRecords);

            var deletedNodeCommand =
                    new Command.NodeCommand(commandSerialization, recordBeforeToDelete, recordAfterDelete);

            commandSerialization.writeNodeCommand(channel, deletedNodeCommand);
            commandSerialization.writeDeletedNodeCommand(channel, deletedNodeCommand);

            var reader = createReader();
            var oldFullNodeCommand = (Command.NodeCommand) reader.read(channel);
            var newDeleteNodeCommand = (Command.NodeCommand) reader.read(channel);

            assertEquals(oldFullNodeCommand.getBefore(), newDeleteNodeCommand.getBefore());
            assertEquals(oldFullNodeCommand.getAfter(), newDeleteNodeCommand.getAfter());
        }
    }

    @RepeatedTest(100)
    void readCreateRelationshipCommand() throws IOException {
        try (var channel = new InMemoryClosableChannel()) {
            var commandSerialization = new LogCommandSerializationV5_11();
            var randomCreated = createRandomRelationshipRecord(7);
            randomCreated.setCreated();
            randomCreated.setInUse(true);

            var relCommand = new Command.RelationshipCommand(commandSerialization, randomCreated, randomCreated);
            commandSerialization.writeCreatedRelationshipCommand(channel, relCommand);

            var reader = createReader();
            var command = reader.read(channel);

            assertThat(command).isInstanceOf(Command.RelationshipCommand.class);
            assertThat(randomCreated).isEqualTo(((Command.RelationshipCommand) command).getAfter());
        }
    }

    @RepeatedTest(100)
    void readDeleteRelationshipCommand() throws IOException {
        try (var channel = new InMemoryClosableChannel()) {
            var commandSerialization = new LogCommandSerializationV5_11();
            var randomDeletedRelationship = createRandomRelationshipRecord(7);
            var relationshipBefore = new RelationshipRecord(randomDeletedRelationship);

            randomDeletedRelationship.setInUse(false);
            randomDeletedRelationship.setCreated(false);

            relationshipBefore.setCreated(false);
            relationshipBefore.setInUse(true);

            var relCommand = new Command.RelationshipCommand(
                    commandSerialization, relationshipBefore, randomDeletedRelationship);
            commandSerialization.writeDeletedRelationshipCommand(channel, relCommand);

            var reader = createReader();
            var command = reader.read(channel);

            assertThat(command).isInstanceOf(Command.RelationshipCommand.class);
            assertThat(relationshipBefore).isEqualTo(((Command.RelationshipCommand) command).getBefore());
        }
    }

    @RepeatedTest(100)
    void createdRelationshipsAreTheSameInNewAndOldCommands() throws IOException {
        try (var channel = new InMemoryClosableChannel()) {
            var commandSerialization = new LogCommandSerializationV5_11();
            var recordBeforeToCreate = new RelationshipRecord(42);
            var recordAfterCreate = createRandomUsedRelationshipRecord(42);

            recordAfterCreate.setCreated(true);

            var relCommand =
                    new Command.RelationshipCommand(commandSerialization, recordBeforeToCreate, recordAfterCreate);

            commandSerialization.writeRelationshipCommand(channel, relCommand);
            commandSerialization.writeCreatedRelationshipCommand(channel, relCommand);

            var reader = createReader();
            var oldFullRelCommand = (Command.RelationshipCommand) reader.read(channel);
            var newCreateRelCommand = (Command.RelationshipCommand) reader.read(channel);

            assertEquals(oldFullRelCommand.getAfter(), newCreateRelCommand.getAfter());
            assertEquals(oldFullRelCommand.getBefore(), newCreateRelCommand.getBefore());
        }
    }

    @RepeatedTest(100)
    void deleteRelationshipsAreTheSameInNewAndOldCommands() throws IOException {
        try (var channel = new InMemoryClosableChannel()) {
            var commandSerialization = new LogCommandSerializationV5_11();
            var recordBeforeToDelete = createRandomUsedRelationshipRecord(42);
            var recordAfterDelete = new RelationshipRecord(42);

            var deletedRelationshipCommand =
                    new Command.RelationshipCommand(commandSerialization, recordBeforeToDelete, recordAfterDelete);

            commandSerialization.writeRelationshipCommand(channel, deletedRelationshipCommand);
            commandSerialization.writeDeletedRelationshipCommand(channel, deletedRelationshipCommand);

            var reader = createReader();
            var oldFullRelCommand = (Command.RelationshipCommand) reader.read(channel);
            var newDeleteRelCommand = (Command.RelationshipCommand) reader.read(channel);

            assertEquals(oldFullRelCommand.getBefore(), newDeleteRelCommand.getBefore());
            assertEquals(oldFullRelCommand.getAfter(), newDeleteRelCommand.getAfter());
        }
    }

    @RepeatedTest(100)
    void readCreatePropertyCommand() throws IOException {
        try (var channel = new InMemoryClosableChannel()) {
            var commandSerialization = new LogCommandSerializationV5_11();
            PropertyRecord randomCreated = createRandomUsedPropertyRecord(7);
            randomCreated.setCreated();

            var propertyCommand = new Command.PropertyCommand(commandSerialization, randomCreated, randomCreated);
            commandSerialization.writeCreatedPropertyCommand(channel, propertyCommand);

            var reader = createReader();
            var command = reader.read(channel);

            assertThat(command).isInstanceOf(Command.PropertyCommand.class);
            assertThat(randomCreated).isEqualTo(((Command.PropertyCommand) command).getAfter());
        }
    }

    @RepeatedTest(100)
    void readDeletePropertyCommand() throws IOException {
        try (var channel = new InMemoryClosableChannel()) {
            var commandSerialization = new LogCommandSerializationV5_11();
            var propertyBefore = createRandomUsedPropertyRecord(7);
            var randomRemovedProperty = new PropertyRecord(propertyBefore);

            randomRemovedProperty.setInUse(false);
            randomRemovedProperty.setCreated(false);
            randomRemovedProperty.clearPropertyBlocks();

            propertyBefore.setCreated(false);

            var propertyCommand =
                    new Command.PropertyCommand(commandSerialization, propertyBefore, randomRemovedProperty);
            commandSerialization.writeDeletedPropertyCommand(channel, propertyCommand);

            var reader = createReader();
            var command = reader.read(channel);

            assertThat(command).isInstanceOf(Command.PropertyCommand.class);
            assertThat(propertyBefore).isEqualTo(((Command.PropertyCommand) command).getBefore());
        }
    }

    @RepeatedTest(100)
    void createdPropertiesAreTheSameInNewAndOldCommands() throws IOException {
        try (var channel = new InMemoryClosableChannel()) {
            var commandSerialization = new LogCommandSerializationV5_11();
            var recordAfterCreate = createRandomUsedPropertyRecord(42);
            var recordBeforeToCreate = new PropertyRecord(recordAfterCreate);

            recordAfterCreate.setCreated(true);

            recordBeforeToCreate.setInUse(false);
            recordBeforeToCreate.setCreated(false);
            recordBeforeToCreate.clearPropertyBlocks();

            var propCommand =
                    new Command.PropertyCommand(commandSerialization, recordBeforeToCreate, recordAfterCreate);

            commandSerialization.writePropertyCommand(channel, propCommand);
            commandSerialization.writeCreatedPropertyCommand(channel, propCommand);

            var reader = createReader();
            var oldFullPropertyCommand = (Command.PropertyCommand) reader.read(channel);
            var newCreatePropertyCommand = (Command.PropertyCommand) reader.read(channel);

            assertEquals(oldFullPropertyCommand.getAfter(), newCreatePropertyCommand.getAfter());
            assertEquals(oldFullPropertyCommand.getBefore(), newCreatePropertyCommand.getBefore());
        }
    }

    @RepeatedTest(100)
    void deletePropertiesAreTheSameInNewAndOldCommands() throws IOException {
        try (var channel = new InMemoryClosableChannel()) {
            var commandSerialization = new LogCommandSerializationV5_11();
            var recordBeforeToDelete = createRandomUsedPropertyRecord(42);
            var recordAfterDelete = new PropertyRecord(recordBeforeToDelete);

            recordBeforeToDelete.setCreated(true);

            recordAfterDelete.setCreated(false);
            recordAfterDelete.setInUse(false);
            recordAfterDelete.clearPropertyBlocks();

            var deletedPropertyCommand =
                    new Command.PropertyCommand(commandSerialization, recordBeforeToDelete, recordAfterDelete);

            commandSerialization.writePropertyCommand(channel, deletedPropertyCommand);
            commandSerialization.writeDeletedPropertyCommand(channel, deletedPropertyCommand);

            var reader = createReader();
            var oldFullPropertyCommand = (Command.PropertyCommand) reader.read(channel);
            var newDeletePropertyCommand = (Command.PropertyCommand) reader.read(channel);

            assertEquals(oldFullPropertyCommand.getBefore(), newDeletePropertyCommand.getBefore());
            assertEquals(oldFullPropertyCommand.getAfter(), newDeletePropertyCommand.getAfter());
        }
    }

    private PropertyRecord createRandomUsedPropertyRecord(long id) {
        return createRandomPropertyRecord(id, true);
    }

    PropertyRecord createRandomPropertyRecord(long id) {
        return createRandomPropertyRecord(id, false);
    }

    PropertyRecord createRandomPropertyRecord(long id, boolean used) {
        var record = new PropertyRecord(id);
        record.initialize(used || random.nextBoolean(), random.nextLong(), random.nextLong());
        if (random.nextBoolean()) {
            record.setCreated();
        }
        if (record.inUse()) {
            PropertyBlock block = new PropertyBlock();
            PropertyStore.encodeValue(block, random.nextInt(1000), Values.of(123), null, null, NULL_CONTEXT, INSTANCE);
            record.addPropertyBlock(block);
        }
        if (random.nextBoolean()) {
            record.addDeletedRecord(new DynamicRecord(random.nextLong(1000)));
        }
        record.setUseFixedReferences(random.nextBoolean());
        switch (random.nextInt(3)) {
            case 0 -> record.setNodeId(44);
            case 1 -> record.setRelId(88);
            default -> record.setSchemaRuleId(11);
        }

        return record;
    }

    NodeRecord createRandomUsedNodeRecord(long id) {
        return createRandomNodeRecord(id, true);
    }

    NodeRecord createRandomNodeRecord(long id) {
        return createRandomNodeRecord(id, random.nextBoolean());
    }

    NodeRecord createRandomNodeRecord(long id, boolean inUse) {
        var record = new NodeRecord(id);
        if (random.nextBoolean()) {
            record.setCreated();
        }
        if (inUse) {
            record.initialize(inUse, random.nextLong(), random.nextBoolean(), random.nextLong(), random.nextLong());
        }

        if (random.nextBoolean()) {
            var labelField = record.getLabelField();
            record.setLabelField(
                    labelField,
                    randomLabelDynamicRecords(id, NodeLabelsField.fieldPointsToDynamicRecordOfLabels(labelField)));
        }

        if (random.nextBoolean()) {
            if (random.nextBoolean()) {
                record.setSecondaryUnitIdOnCreate(random.nextLong(1000));
            } else {
                record.setSecondaryUnitIdOnLoad(random.nextLong(1000));
            }
        }
        record.setUseFixedReferences(random.nextBoolean());
        return record;
    }

    private List<DynamicRecord> randomLabelDynamicRecords(long nodeId, boolean mustIncludeUsed) {
        if (mustIncludeUsed || random.nextBoolean()) {
            var labels = random.random()
                    .longs()
                    .limit(random.nextInt(1, 10))
                    .sorted()
                    .toArray();
            var records = DynamicNodeLabels.allocateRecordsForDynamicLabels(
                    nodeId, labels, new RandomizedDynamicRecordAllocator(), NULL_CONTEXT, INSTANCE);
            if (mustIncludeUsed) {
                records.get(0).setInUse(true);
            }
            return records;
        }
        return emptyList();
    }

    RelationshipRecord createRandomUsedRelationshipRecord(long id) {
        return createRandomRelationshipRecord(id, true);
    }

    RelationshipRecord createRandomRelationshipRecord(long id) {
        return createRandomRelationshipRecord(id, random.nextBoolean());
    }

    RelationshipRecord createRandomRelationshipRecord(long id, boolean inUse) {
        var record = new RelationshipRecord(id);
        if (random.nextBoolean()) {
            record.setCreated();
        }
        if (inUse) {
            record.initialize(
                    inUse,
                    random.nextLong(),
                    random.nextLong(),
                    random.nextLong(),
                    random.nextInt(),
                    random.nextLong(),
                    random.nextLong(),
                    random.nextLong(),
                    random.nextLong(),
                    random.nextBoolean(),
                    random.nextBoolean());
        }

        if (random.nextBoolean()) {
            if (random.nextBoolean()) {
                record.setSecondaryUnitIdOnCreate(random.nextLong(1000));
            } else {
                record.setSecondaryUnitIdOnLoad(random.nextLong(1000));
            }
        }
        record.setUseFixedReferences(random.nextBoolean());
        return record;
    }

    private class RandomizedDynamicRecordAllocator implements DynamicRecordAllocator {
        private long idGenerator = 1;

        @Override
        public int getRecordDataSize() {
            return 23;
        }

        @Override
        public DynamicRecord nextRecord(CursorContext cursorContext) {
            var dynamicRecord = new DynamicRecord(idGenerator++);
            dynamicRecord.setInUse(random.nextBoolean());
            dynamicRecord.setCreated(random.nextBoolean());
            return dynamicRecord;
        }
    }
}
