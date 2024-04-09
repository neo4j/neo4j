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
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.neo4j.io.pagecache.context.CursorContext.NULL_CONTEXT;
import static org.neo4j.kernel.impl.store.record.Record.NULL_REFERENCE;
import static org.neo4j.memory.EmptyMemoryTracker.INSTANCE;

import java.io.IOException;
import java.util.List;
import java.util.function.Consumer;
import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.neo4j.internal.kernel.api.connectioninfo.ClientConnectionInfo;
import org.neo4j.internal.kernel.api.security.AuthSubject;
import org.neo4j.internal.kernel.api.security.SecurityContext;
import org.neo4j.internal.schema.IndexPrototype;
import org.neo4j.internal.schema.SchemaDescriptors;
import org.neo4j.internal.schema.SchemaRule;
import org.neo4j.io.pagecache.context.CursorContext;
import org.neo4j.kernel.impl.store.DynamicNodeLabels;
import org.neo4j.kernel.impl.store.DynamicRecordAllocator;
import org.neo4j.kernel.impl.store.NodeLabelsField;
import org.neo4j.kernel.impl.store.PropertyStore;
import org.neo4j.kernel.impl.store.PropertyType;
import org.neo4j.kernel.impl.store.record.AbstractBaseRecord;
import org.neo4j.kernel.impl.store.record.DynamicRecord;
import org.neo4j.kernel.impl.store.record.LabelTokenRecord;
import org.neo4j.kernel.impl.store.record.MetaDataRecord;
import org.neo4j.kernel.impl.store.record.NodeRecord;
import org.neo4j.kernel.impl.store.record.PropertyBlock;
import org.neo4j.kernel.impl.store.record.PropertyKeyTokenRecord;
import org.neo4j.kernel.impl.store.record.PropertyRecord;
import org.neo4j.kernel.impl.store.record.RelationshipGroupRecord;
import org.neo4j.kernel.impl.store.record.RelationshipRecord;
import org.neo4j.kernel.impl.store.record.RelationshipTypeTokenRecord;
import org.neo4j.kernel.impl.store.record.SchemaRecord;
import org.neo4j.kernel.impl.transaction.log.InMemoryClosableChannel;
import org.neo4j.storageengine.api.CommandReader;
import org.neo4j.storageengine.api.StorageCommand;
import org.neo4j.test.LatestVersions;
import org.neo4j.test.RandomSupport;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.RandomExtension;
import org.neo4j.values.storable.Values;

@ExtendWith(RandomExtension.class)
abstract class LogCommandSerializationV5Base {

    @Inject
    RandomSupport random;

    static final long NULL_REF = NULL_REFERENCE.longValue();

    @Test
    void shouldReadPropertyKeyCommand() throws Exception {
        // Given
        InMemoryClosableChannel channel = new InMemoryClosableChannel();
        PropertyKeyTokenRecord before = new PropertyKeyTokenRecord(42);
        PropertyKeyTokenRecord after = new PropertyKeyTokenRecord(before);
        after.initialize(true, 13);
        after.setCreated();
        new Command.PropertyKeyTokenCommand(writer(), before, after).serialize(channel);

        // When
        CommandReader reader = createReader();
        StorageCommand command = reader.read(channel);
        assertTrue(command instanceof Command.PropertyKeyTokenCommand);

        Command.PropertyKeyTokenCommand propertyKeyTokenCommand = (Command.PropertyKeyTokenCommand) command;

        // Then
        assertBeforeAndAfterEquals(propertyKeyTokenCommand, before, after);
    }

    @Test
    void shouldReadInternalPropertyKeyCommand() throws Exception {
        // Given
        InMemoryClosableChannel channel = new InMemoryClosableChannel();
        PropertyKeyTokenRecord before = new PropertyKeyTokenRecord(42);
        PropertyKeyTokenRecord after = new PropertyKeyTokenRecord(before);
        after.initialize(true, 13);
        after.setCreated();
        after.setInternal(true);
        new Command.PropertyKeyTokenCommand(writer(), before, after).serialize(channel);

        // When
        CommandReader reader = createReader();
        StorageCommand command = reader.read(channel);
        assertTrue(command instanceof Command.PropertyKeyTokenCommand);

        Command.PropertyKeyTokenCommand propertyKeyTokenCommand = (Command.PropertyKeyTokenCommand) command;

        // Then
        assertBeforeAndAfterEquals(propertyKeyTokenCommand, before, after);
    }

    @Test
    void shouldReadLabelCommand() throws Exception {
        // Given
        InMemoryClosableChannel channel = new InMemoryClosableChannel();
        LabelTokenRecord before = new LabelTokenRecord(42);
        LabelTokenRecord after = new LabelTokenRecord(before);
        after.initialize(true, 13);
        after.setCreated();
        new Command.LabelTokenCommand(writer(), before, after).serialize(channel);

        // When
        CommandReader reader = createReader();
        StorageCommand command = reader.read(channel);
        assertTrue(command instanceof Command.LabelTokenCommand);

        Command.LabelTokenCommand labelTokenCommand = (Command.LabelTokenCommand) command;

        // Then
        assertBeforeAndAfterEquals(labelTokenCommand, before, after);
    }

    @Test
    void shouldReadInternalLabelCommand() throws Exception {
        // Given
        InMemoryClosableChannel channel = new InMemoryClosableChannel();
        LabelTokenRecord before = new LabelTokenRecord(42);
        LabelTokenRecord after = new LabelTokenRecord(before);
        after.initialize(true, 13);
        after.setCreated();
        after.setInternal(true);
        new Command.LabelTokenCommand(writer(), before, after).serialize(channel);

        // When
        CommandReader reader = createReader();
        StorageCommand command = reader.read(channel);
        assertTrue(command instanceof Command.LabelTokenCommand);

        Command.LabelTokenCommand labelTokenCommand = (Command.LabelTokenCommand) command;

        // Then
        assertBeforeAndAfterEquals(labelTokenCommand, before, after);
    }

    @Test
    void shouldReadRelationshipTypeCommand() throws Exception {
        // Given
        InMemoryClosableChannel channel = new InMemoryClosableChannel();
        RelationshipTypeTokenRecord before = new RelationshipTypeTokenRecord(42);
        RelationshipTypeTokenRecord after = new RelationshipTypeTokenRecord(before);
        after.initialize(true, 13);
        after.setCreated();
        new Command.RelationshipTypeTokenCommand(writer(), before, after).serialize(channel);

        // When
        CommandReader reader = createReader();
        StorageCommand command = reader.read(channel);
        assertTrue(command instanceof Command.RelationshipTypeTokenCommand);

        Command.RelationshipTypeTokenCommand relationshipTypeTokenCommand =
                (Command.RelationshipTypeTokenCommand) command;

        // Then
        assertBeforeAndAfterEquals(relationshipTypeTokenCommand, before, after);
    }

    @Test
    void shouldReadInternalRelationshipTypeLabelCommand() throws Exception {
        // Given
        InMemoryClosableChannel channel = new InMemoryClosableChannel();
        RelationshipTypeTokenRecord before = new RelationshipTypeTokenRecord(42);
        RelationshipTypeTokenRecord after = new RelationshipTypeTokenRecord(before);
        after.initialize(true, 13);
        after.setCreated();
        after.setInternal(true);
        new Command.RelationshipTypeTokenCommand(writer(), before, after).serialize(channel);

        // When
        CommandReader reader = createReader();
        StorageCommand command = reader.read(channel);
        assertTrue(command instanceof Command.RelationshipTypeTokenCommand);

        Command.RelationshipTypeTokenCommand relationshipTypeTokenCommand =
                (Command.RelationshipTypeTokenCommand) command;

        // Then
        assertBeforeAndAfterEquals(relationshipTypeTokenCommand, before, after);
    }

    @Test
    void shouldReadRelationshipCommand() throws Throwable {
        // Given
        InMemoryClosableChannel channel = new InMemoryClosableChannel();
        RelationshipRecord before = new RelationshipRecord(42);
        before.setLinks(-1, -1, -1);
        RelationshipRecord after = new RelationshipRecord(42);
        after.initialize(true, 0, 1, 2, 3, 4, 5, 6, 7, true, true);
        after.setCreated();
        new Command.RelationshipCommand(writer(), before, after).serialize(channel);

        // When
        CommandReader reader = createReader();
        StorageCommand command = reader.read(channel);
        assertTrue(command instanceof Command.RelationshipCommand);

        Command.RelationshipCommand relationshipCommand = (Command.RelationshipCommand) command;

        // Then
        assertBeforeAndAfterEquals(relationshipCommand, before, after);
    }

    @Test
    void readRelationshipCommandWithSecondaryUnit() throws IOException {
        InMemoryClosableChannel channel = new InMemoryClosableChannel();
        RelationshipRecord before = new RelationshipRecord(42);
        before.initialize(true, 0, 1, 2, 3, 4, 5, 6, 7, true, true);
        before.setSecondaryUnitIdOnLoad(47);
        RelationshipRecord after = new RelationshipRecord(42);
        after.initialize(true, 0, 1, 8, 3, 4, 5, 6, 7, true, true);
        new Command.RelationshipCommand(writer(), before, after).serialize(channel);

        CommandReader reader = createReader();
        StorageCommand command = reader.read(channel);
        assertTrue(command instanceof Command.RelationshipCommand);

        Command.RelationshipCommand relationshipCommand = (Command.RelationshipCommand) command;
        assertBeforeAndAfterEquals(relationshipCommand, before, after);
    }

    @Test
    void readRelationshipCommandWithNonRequiredSecondaryUnit() throws IOException {
        InMemoryClosableChannel channel = new InMemoryClosableChannel();
        RelationshipRecord before = new RelationshipRecord(42);
        before.initialize(true, 0, 1, 2, 3, 4, 5, 6, 7, true, true);
        before.setSecondaryUnitIdOnLoad(52);
        RelationshipRecord after = new RelationshipRecord(42);
        after.initialize(true, 0, 1, 8, 3, 4, 5, 6, 7, true, true);
        new Command.RelationshipCommand(writer(), before, after).serialize(channel);

        CommandReader reader = createReader();
        StorageCommand command = reader.read(channel);
        assertTrue(command instanceof Command.RelationshipCommand);

        Command.RelationshipCommand relationshipCommand = (Command.RelationshipCommand) command;
        assertBeforeAndAfterEquals(relationshipCommand, before, after);
    }

    @Test
    void readRelationshipCommandWithFixedReferenceFormat() throws IOException {
        InMemoryClosableChannel channel = new InMemoryClosableChannel();
        RelationshipRecord before = new RelationshipRecord(42);
        before.initialize(true, 0, 1, 2, 3, 4, 5, 6, 7, true, true);
        before.setUseFixedReferences(true);
        RelationshipRecord after = new RelationshipRecord(42);
        after.initialize(true, 0, 1, 8, 3, 4, 5, 6, 7, true, true);
        after.setUseFixedReferences(true);
        new Command.RelationshipCommand(writer(), before, after).serialize(channel);

        CommandReader reader = createReader();
        StorageCommand command = reader.read(channel);
        assertTrue(command instanceof Command.RelationshipCommand);

        Command.RelationshipCommand relationshipCommand = (Command.RelationshipCommand) command;
        assertBeforeAndAfterEquals(relationshipCommand, before, after);
        assertTrue(relationshipCommand.getBefore().isUseFixedReferences());
        assertTrue(relationshipCommand.getAfter().isUseFixedReferences());
    }

    @Test
    void shouldReadRelationshipGroupCommand() throws Throwable {
        // Given
        InMemoryClosableChannel channel = new InMemoryClosableChannel();
        RelationshipGroupRecord before =
                new RelationshipGroupRecord(42).initialize(false, 3, NULL_REF, NULL_REF, NULL_REF, NULL_REF, NULL_REF);
        RelationshipGroupRecord after = new RelationshipGroupRecord(42).initialize(true, 3, 4, 5, 6, 7, 8);
        after.setCreated();

        new Command.RelationshipGroupCommand(writer(), before, after).serialize(channel);

        // When
        CommandReader reader = createReader();
        StorageCommand command = reader.read(channel);
        assertTrue(command instanceof Command.RelationshipGroupCommand);

        Command.RelationshipGroupCommand relationshipGroupCommand = (Command.RelationshipGroupCommand) command;

        // Then
        assertBeforeAndAfterEquals(relationshipGroupCommand, before, after);
    }

    @Test
    void readRelationshipGroupCommandWithSecondaryUnit() throws IOException {
        // Given
        InMemoryClosableChannel channel = new InMemoryClosableChannel();
        RelationshipGroupRecord before =
                new RelationshipGroupRecord(42).initialize(false, 3, NULL_REF, NULL_REF, NULL_REF, NULL_REF, NULL_REF);
        RelationshipGroupRecord after = new RelationshipGroupRecord(42).initialize(true, 3, 4, 5, 6, 7, 8);
        after.setSecondaryUnitIdOnCreate(17);
        after.setCreated();

        new Command.RelationshipGroupCommand(writer(), before, after).serialize(channel);

        // When
        CommandReader reader = createReader();
        StorageCommand command = reader.read(channel);
        assertTrue(command instanceof Command.RelationshipGroupCommand);

        Command.RelationshipGroupCommand relationshipGroupCommand = (Command.RelationshipGroupCommand) command;

        // Then
        assertBeforeAndAfterEquals(relationshipGroupCommand, before, after);
    }

    @Test
    void readRelationshipGroupCommandWithNonRequiredSecondaryUnit() throws IOException {
        // Given
        InMemoryClosableChannel channel = new InMemoryClosableChannel();
        RelationshipGroupRecord before =
                new RelationshipGroupRecord(42).initialize(false, 3, NULL_REF, NULL_REF, NULL_REF, NULL_REF, NULL_REF);
        RelationshipGroupRecord after = new RelationshipGroupRecord(42).initialize(true, 3, 4, 5, 6, 7, 8);
        after.setSecondaryUnitIdOnCreate(17);
        after.setCreated();

        new Command.RelationshipGroupCommand(writer(), before, after).serialize(channel);

        // When
        CommandReader reader = createReader();
        StorageCommand command = reader.read(channel);
        assertTrue(command instanceof Command.RelationshipGroupCommand);

        Command.RelationshipGroupCommand relationshipGroupCommand = (Command.RelationshipGroupCommand) command;

        // Then
        assertBeforeAndAfterEquals(relationshipGroupCommand, before, after);
    }

    @Test
    void readRelationshipGroupCommandWithFixedReferenceFormat() throws IOException {
        // Given
        InMemoryClosableChannel channel = new InMemoryClosableChannel();
        RelationshipGroupRecord before =
                new RelationshipGroupRecord(42).initialize(false, 3, NULL_REF, NULL_REF, NULL_REF, NULL_REF, NULL_REF);
        before.setUseFixedReferences(true);
        RelationshipGroupRecord after = new RelationshipGroupRecord(42).initialize(true, 3, 4, 5, 6, 7, 8);
        after.setUseFixedReferences(true);
        after.setCreated();

        new Command.RelationshipGroupCommand(writer(), before, after).serialize(channel);

        // When
        CommandReader reader = createReader();
        StorageCommand command = reader.read(channel);
        assertTrue(command instanceof Command.RelationshipGroupCommand);

        Command.RelationshipGroupCommand relationshipGroupCommand = (Command.RelationshipGroupCommand) command;

        // Then
        assertBeforeAndAfterEquals(relationshipGroupCommand, before, after);
        assertTrue(relationshipGroupCommand.getBefore().isUseFixedReferences());
        assertTrue(relationshipGroupCommand.getAfter().isUseFixedReferences());
    }

    @Test
    public void readRelationshipGroupWithBiggerThanShortRelationshipType() throws IOException {
        // Given
        InMemoryClosableChannel channel = new InMemoryClosableChannel();
        RelationshipGroupRecord before =
                new RelationshipGroupRecord(42).initialize(false, 3, NULL_REF, NULL_REF, NULL_REF, NULL_REF, NULL_REF);
        RelationshipGroupRecord after =
                new RelationshipGroupRecord(42).initialize(true, (1 << Short.SIZE) + 10, 4, 5, 6, 7, 8);
        after.setCreated();

        new Command.RelationshipGroupCommand(writer(), before, after).serialize(channel);

        // When
        CommandReader reader = createReader();
        StorageCommand command = reader.read(channel);
        assertTrue(command instanceof Command.RelationshipGroupCommand);

        Command.RelationshipGroupCommand relationshipGroupCommand = (Command.RelationshipGroupCommand) command;

        // Then
        assertBeforeAndAfterEquals(relationshipGroupCommand, before, after);
    }

    @Test
    void nodeCommandWithFixedReferenceFormat() throws Exception {
        // Given
        InMemoryClosableChannel channel = new InMemoryClosableChannel();
        NodeRecord before = new NodeRecord(42).initialize(true, 99, false, 33, 66);
        NodeRecord after = new NodeRecord(42).initialize(true, 99, false, 33, 66);
        before.setUseFixedReferences(true);
        after.setUseFixedReferences(true);

        new Command.NodeCommand(writer(), before, after).serialize(channel);

        // When
        CommandReader reader = createReader();
        StorageCommand command = reader.read(channel);
        assertTrue(command instanceof Command.NodeCommand);

        Command.NodeCommand nodeCommand = (Command.NodeCommand) command;

        // Then
        assertBeforeAndAfterEquals(nodeCommand, before, after);
        assertTrue(nodeCommand.getBefore().isUseFixedReferences());
        assertTrue(nodeCommand.getAfter().isUseFixedReferences());
    }

    @Test
    void readPropertyCommandWithFixedReferenceFormat() throws IOException {
        InMemoryClosableChannel channel = new InMemoryClosableChannel();
        PropertyRecord before = new PropertyRecord(1);
        PropertyRecord after = new PropertyRecord(1);
        before.setUseFixedReferences(true);
        after.setUseFixedReferences(true);

        new Command.PropertyCommand(writer(), before, after).serialize(channel);

        CommandReader reader = createReader();
        StorageCommand command = reader.read(channel);
        assertTrue(command instanceof Command.PropertyCommand);

        Command.PropertyCommand propertyCommand = (Command.PropertyCommand) command;

        // Then
        assertBeforeAndAfterEquals(propertyCommand, before, after);
        assertTrue(propertyCommand.getBefore().isUseFixedReferences());
        assertTrue(propertyCommand.getAfter().isUseFixedReferences());
    }

    @Test
    void shouldReadSomeCommands() throws Exception {
        // GIVEN
        LogCommandSerialization writer = writer();
        InMemoryClosableChannel channel = new InMemoryClosableChannel();
        writer.writeNodeCommand(channel, Commands.createNode(0));
        writer.writeNodeCommand(channel, Commands.createNode(1));
        writer.writeRelationshipTypeTokenCommand(channel, Commands.createRelationshipTypeToken(0, 0));
        writer.writeRelationshipCommand(channel, Commands.createRelationship(0, 0, 1, 0));
        writer.writePropertyKeyTokenCommand(channel, Commands.createPropertyKeyToken(0, 0));
        writer.writePropertyCommand(channel, Commands.createProperty(0, PropertyType.SHORT_STRING, 0));
        CommandReader reader = createReader();

        // THEN
        assertTrue(reader.read(channel) instanceof Command.NodeCommand);
        assertTrue(reader.read(channel) instanceof Command.NodeCommand);
        assertTrue(reader.read(channel) instanceof Command.RelationshipTypeTokenCommand);
        assertTrue(reader.read(channel) instanceof Command.RelationshipCommand);
        assertTrue(reader.read(channel) instanceof Command.PropertyKeyTokenCommand);
        assertTrue(reader.read(channel) instanceof Command.PropertyCommand);
    }

    @Test
    void shouldReadSchemaCommand() throws Exception {
        // given
        InMemoryClosableChannel channel = new InMemoryClosableChannel();
        SchemaRecord before = new SchemaRecord(42);
        SchemaRecord after = new SchemaRecord(before);
        after.initialize(true, 353);
        after.setConstraint(true);
        after.setCreated();

        long id = after.getId();
        SchemaRule rule = IndexPrototype.forSchema(SchemaDescriptors.forLabel(1, 2, 3))
                .withName("index_" + id)
                .materialise(id);
        writer().writeSchemaRuleCommand(channel, new Command.SchemaRuleCommand(writer(), before, after, rule));

        CommandReader reader = createReader();
        Command.SchemaRuleCommand command = (Command.SchemaRuleCommand) reader.read(channel);

        assertBeforeAndAfterEquals(command, before, after);
    }

    @Test
    void shouldReadAndWriteMetaDataCommand() throws IOException {
        // Given
        InMemoryClosableChannel channel = new InMemoryClosableChannel();
        MetaDataRecord before = new MetaDataRecord();
        MetaDataRecord after = new MetaDataRecord();
        after.initialize(true, 999);
        new Command.MetaDataCommand(writer(), before, after).serialize(channel);

        // When
        CommandReader reader = createReader();
        StorageCommand command = reader.read(channel);
        assertTrue(command instanceof Command.MetaDataCommand);

        Command.MetaDataCommand readCommand = (Command.MetaDataCommand) command;

        // Then
        assertBeforeAndAfterEquals(readCommand, before, after);
    }

    @Test
    void shouldReadRelationshipGroupCommandIncludingExternalDegrees() throws Throwable {
        // Given
        InMemoryClosableChannel channel = new InMemoryClosableChannel();
        RelationshipGroupRecord before =
                new RelationshipGroupRecord(42).initialize(false, 3, NULL_REF, NULL_REF, NULL_REF, NULL_REF, NULL_REF);
        RelationshipGroupRecord after = new RelationshipGroupRecord(42).initialize(true, 3, 4, 5, 6, 7, 8);
        after.setHasExternalDegreesOut(random.nextBoolean());
        after.setHasExternalDegreesIn(random.nextBoolean());
        after.setHasExternalDegreesLoop(random.nextBoolean());
        after.setCreated();

        new Command.RelationshipGroupCommand(writer(), before, after).serialize(channel);

        // When
        CommandReader reader = createReader();
        StorageCommand command = reader.read(channel);
        assertTrue(command instanceof Command.RelationshipGroupCommand);

        Command.RelationshipGroupCommand relationshipGroupCommand = (Command.RelationshipGroupCommand) command;

        // Then
        assertBeforeAndAfterEquals(relationshipGroupCommand, before, after);
    }

    @Test
    void shouldReadRelationshipGroupWithLargerThanShortTypeIncludingExternalDegrees() throws Throwable {
        // Given
        InMemoryClosableChannel channel = new InMemoryClosableChannel();
        RelationshipGroupRecord before =
                new RelationshipGroupRecord(42).initialize(false, 3, NULL_REF, NULL_REF, NULL_REF, NULL_REF, NULL_REF);
        RelationshipGroupRecord after =
                new RelationshipGroupRecord(42).initialize(true, (1 << Short.SIZE) + 10, 4, 5, 6, 7, 8);
        after.setHasExternalDegreesOut(random.nextBoolean());
        after.setHasExternalDegreesIn(random.nextBoolean());
        after.setHasExternalDegreesLoop(random.nextBoolean());
        after.setCreated();

        new Command.RelationshipGroupCommand(writer(), before, after).serialize(channel);

        // When
        CommandReader reader = createReader();
        StorageCommand command = reader.read(channel);
        assertTrue(command instanceof Command.RelationshipGroupCommand);

        Command.RelationshipGroupCommand relationshipGroupCommand = (Command.RelationshipGroupCommand) command;

        // Then
        assertBeforeAndAfterEquals(relationshipGroupCommand, before, after);
    }

    @RepeatedTest(10)
    void propertyKeyCommand() throws Exception {
        testDoubleSerialization(Command.PropertyKeyTokenCommand.class, createRandomPropertyKeyTokenCommand());
    }

    Command.PropertyKeyTokenCommand createRandomPropertyKeyTokenCommand() {
        var id = random.nextInt();
        var before = createRandomPropertyKeyTokenRecord(id);
        var after = createRandomPropertyKeyTokenRecord(id);
        return new Command.PropertyKeyTokenCommand(writer(), before, after);
    }

    PropertyKeyTokenRecord createRandomPropertyKeyTokenRecord(int id) {
        var record = new PropertyKeyTokenRecord(id);
        record.initialize(random.nextBoolean(), random.nextInt(), random.nextInt());
        record.setInternal(random.nextBoolean());
        record.setCreated(random.nextBoolean());
        addDynamicRecords(record::addNameRecord);
        return record;
    }

    @RepeatedTest(10)
    void labelTokenCommand() throws Exception {
        testDoubleSerialization(Command.LabelTokenCommand.class, createRandomLabelTokenCommand());
    }

    Command.LabelTokenCommand createRandomLabelTokenCommand() {
        var id = random.nextInt();
        var before = createRandomLabelTokenRecord(id);
        var after = createRandomLabelTokenRecord(id);
        return new Command.LabelTokenCommand(writer(), before, after);
    }

    LabelTokenRecord createRandomLabelTokenRecord(int id) {
        var record = new LabelTokenRecord(id);
        record.initialize(random.nextBoolean(), random.nextInt());
        record.setInternal(random.nextBoolean());
        record.setCreated(random.nextBoolean());
        addDynamicRecords(record::addNameRecord);
        return record;
    }

    @RepeatedTest(10)
    void relationshipTypeTokenCommand() throws Exception {
        testDoubleSerialization(Command.RelationshipTypeTokenCommand.class, createRandomRelationshipTypeTokenCommand());
    }

    Command.RelationshipTypeTokenCommand createRandomRelationshipTypeTokenCommand() {
        var id = random.nextInt();
        var before = createRandomRelationshipTypeTokenRecord(id);
        var after = createRandomRelationshipTypeTokenRecord(id);
        return new Command.RelationshipTypeTokenCommand(writer(), before, after);
    }

    RelationshipTypeTokenRecord createRandomRelationshipTypeTokenRecord(int id) {
        var record = new RelationshipTypeTokenRecord(id);
        record.initialize(random.nextBoolean(), random.nextInt());
        record.setInternal(random.nextBoolean());
        record.setCreated(random.nextBoolean());
        addDynamicRecords(record::addNameRecord);

        return record;
    }

    private void addDynamicRecords(Consumer<DynamicRecord> consumer) {
        int limit = random.nextInt(10);
        for (int i = 0; i < limit; i++) {
            consumer.accept(createRandomDynamicRecord());
        }
    }

    private DynamicRecord createRandomDynamicRecord() {
        var dynamicRecord = new DynamicRecord(random.nextLong(Integer.MAX_VALUE));
        dynamicRecord.setInUse(random.nextBoolean());

        if (dynamicRecord.inUse()) {
            dynamicRecord.setType(random.nextInt());
            dynamicRecord.setNextBlock(random.nextLong(Integer.MAX_VALUE));
            dynamicRecord.setStartRecord(random.nextBoolean());
            dynamicRecord.setData(random.nextBytes(new byte[29]));
        }

        return dynamicRecord;
    }

    @RepeatedTest(10)
    void schemaCommandSerialization() throws IOException {
        testDoubleSerialization(Command.SchemaRuleCommand.class, createRandomSchemaCommand());
    }

    Command.SchemaRuleCommand createRandomSchemaCommand() {
        var id = Math.abs(random.nextLong());
        var before = createRandomSchemaRecord(id);
        var after = createRandomSchemaRecord(id);
        SchemaRule rule = IndexPrototype.forSchema(SchemaDescriptors.forLabel(1, 2, 3))
                .withName("index_" + id)
                .materialise(id);
        return new Command.SchemaRuleCommand(writer(), before, after, rule);
    }

    SchemaRecord createRandomSchemaRecord(long id) {
        var record = new SchemaRecord(id);
        var inUse = random.nextBoolean();
        record.initialize(inUse, inUse ? random.nextLong() : -1);
        if (random.nextBoolean()) {
            record.setCreated();
        }
        if (inUse) {
            record.setConstraint(random.nextBoolean());
        }
        return record;
    }

    PropertyRecord createRandomPropertyRecord(long id) {
        var record = new PropertyRecord(id);
        record.initialize(random.nextBoolean(), random.nextLong(), random.nextLong());
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

    NodeRecord createRandomNodeRecord(long id) {
        var record = new NodeRecord(id);
        var inUse = random.nextBoolean();
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
            var labels =
                    random.random().ints().limit(random.nextInt(1, 10)).sorted().toArray();
            var records = DynamicNodeLabels.allocateRecordsForDynamicLabels(
                    nodeId, labels, new RandomizedDynamicRecordAllocator(), NULL_CONTEXT, INSTANCE);
            if (mustIncludeUsed) {
                records.get(0).setInUse(true);
            }
            return records;
        }
        return emptyList();
    }

    RelationshipRecord createRandomRelationshipRecord(long id) {
        var record = new RelationshipRecord(id);
        var inUse = random.nextBoolean();
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

    @RepeatedTest(10)
    void relationshipGroupCommandSerialization() throws IOException {
        testDoubleSerialization(Command.RelationshipGroupCommand.class, createRandomRelationshipGroup());
    }

    private Command.RelationshipGroupCommand createRandomRelationshipGroup() {
        var id = Math.abs(random.nextLong());
        var before = createRandomRelationshipGroupRecord(id);
        var after = createRandomRelationshipGroupRecord(id);
        return new Command.RelationshipGroupCommand(writer(), before, after);
    }

    RelationshipGroupRecord createRandomRelationshipGroupRecord(long id) {
        var record = new RelationshipGroupRecord(id);
        record.initialize(
                random.nextBoolean(),
                random.nextInt(0xffffff),
                random.nextLong(),
                random.nextLong(),
                random.nextLong(),
                random.nextLong(),
                random.nextLong());

        record.setCreated(random.nextBoolean());
        record.setHasExternalDegreesOut(random.nextBoolean());
        record.setHasExternalDegreesIn(random.nextBoolean());
        record.setHasExternalDegreesLoop(random.nextBoolean());

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

    /**
     * The purpose of this test is to verify that serialization of deserialized command produces the same checksum. This test doesn't assert equality of
     * original and deserialized commands, as input supposed to be randomly generated and can produce records that contain information that will not be
     * serialized. I.e. serialization of record not in use can skip irrelevant information. On the other side, if something is written into the tx log, it must
     * be read during deserialization
     * <p>
     * Possible improvement: validate that original record and deserialized record are applied to store they produce equal data.
     */
    <T extends Command.BaseCommand<?>> void testDoubleSerialization(Class<T> type, T original) throws IOException {
        byte version = LatestVersions.LATEST_KERNEL_VERSION.version();
        InMemoryClosableChannel originalChannel = new InMemoryClosableChannel();

        originalChannel.beginChecksum();
        originalChannel.putVersion(version);
        original.serialize(originalChannel);
        var originalChecksum = originalChannel.putChecksum();

        // When
        CommandReader reader = createReader();
        byte readOnceVersion = originalChannel.getVersion();
        assertThat(readOnceVersion).isEqualTo(version);
        var readOnce = (Command.BaseCommand<?>) reader.read(originalChannel);
        assertThat(readOnce).isInstanceOf(type);

        var anotherChannel = new InMemoryClosableChannel();
        anotherChannel.beginChecksum();
        anotherChannel.putVersion(version);
        readOnce.serialize(anotherChannel);
        var anotherChecksum = anotherChannel.putChecksum();

        byte readTwiceVersion = anotherChannel.getVersion();
        assertThat(readTwiceVersion).isEqualTo(version);
        var readTwice = (Command.BaseCommand<?>) reader.read(anotherChannel);
        assertThat(readTwice).isInstanceOf(type);
        assertThat(originalChecksum)
                .as("Checksums must be equal after double serialization \n" + "Original: "
                        + original + "\n" + "Read once: "
                        + readOnce + "\n" + "Read twice: "
                        + readTwice)
                .isEqualTo(anotherChecksum);
        assertCommandsEqual(original, readOnce);
        assertCommandsEqual(readOnce, readTwice);
    }

    private static void assertCommandsEqual(Command.BaseCommand<?> left, Command.BaseCommand<?> right) {
        assertEqualsIncludingFlags(left.getBefore(), right.getBefore());
        assertEqualsIncludingFlags(left.getAfter(), right.getAfter());
    }

    private static void assertEqualsIncludingFlags(AbstractBaseRecord left, AbstractBaseRecord right) {
        assertThat(left).isEqualTo(right);
        assertThat(left.isCreated())
                .as("Created flag mismatch:\nleft " + left + " \nright " + right)
                .isEqualTo(right.isCreated());
        assertThat(left.isUseFixedReferences())
                .as("Fixed references flag mismatch:\nleft " + left + " \nright " + right)
                .isEqualTo(right.isUseFixedReferences());
        assertThat(left.getSecondaryUnitId())
                .as("Secondary unit id mismatch:\nleft " + left + " \nright " + right)
                .isEqualTo(right.getSecondaryUnitId());
        assertThat(left.requiresSecondaryUnit())
                .as("Secondary unit required flag mismatch:\nleft " + left + " \nright " + right)
                .isEqualTo(right.requiresSecondaryUnit());
        assertThat(left.isSecondaryUnitCreated())
                .as("Secondary unit created flag mismatch:\nleft " + left + " \nright " + right)
                .isEqualTo(right.isSecondaryUnitCreated());
    }

    private static <RECORD extends AbstractBaseRecord> void assertBeforeAndAfterEquals(
            Command.BaseCommand<RECORD> command, RECORD before, RECORD after) {
        assertEqualsIncludingFlags(before, command.getBefore());
        assertEqualsIncludingFlags(after, command.getAfter());
    }

    static SecurityContext securityContext() {
        final var subject = subject();
        final var securityContext = mock(SecurityContext.class);
        when(securityContext.subject()).thenReturn(subject);
        when(securityContext.connectionInfo()).thenReturn(ClientConnectionInfo.EMBEDDED_CONNECTION);
        return securityContext;
    }

    private static AuthSubject subject() {
        final var authSubject = mock(AuthSubject.class);
        when(authSubject.authenticatedUser()).thenReturn("me");
        when(authSubject.executingUser()).thenReturn("me");
        return authSubject;
    }

    abstract CommandReader createReader();

    abstract LogCommandSerialization writer();

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
