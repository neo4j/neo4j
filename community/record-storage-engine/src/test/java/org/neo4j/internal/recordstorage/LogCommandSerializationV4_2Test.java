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

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.kernel.impl.store.record.Record.NULL_REFERENCE;

import java.io.IOException;
import java.nio.ByteOrder;
import org.junit.jupiter.api.Test;
import org.neo4j.kernel.impl.store.record.AbstractBaseRecord;
import org.neo4j.kernel.impl.store.record.LabelTokenRecord;
import org.neo4j.kernel.impl.store.record.NodeRecord;
import org.neo4j.kernel.impl.store.record.PropertyKeyTokenRecord;
import org.neo4j.kernel.impl.store.record.PropertyRecord;
import org.neo4j.kernel.impl.store.record.RelationshipGroupRecord;
import org.neo4j.kernel.impl.store.record.RelationshipRecord;
import org.neo4j.kernel.impl.store.record.RelationshipTypeTokenRecord;
import org.neo4j.kernel.impl.store.record.SchemaRecord;
import org.neo4j.kernel.impl.transaction.log.InMemoryClosableChannel;
import org.neo4j.storageengine.api.CommandReader;
import org.neo4j.storageengine.api.StorageCommand;

class LogCommandSerializationV4_2Test {
    static final long NULL_REF = NULL_REFERENCE.longValue();

    @Test
    void shouldReadPropertyKeyCommand() throws Exception {
        // Given
        PropertyKeyTokenRecord before = new PropertyKeyTokenRecord(42);
        PropertyKeyTokenRecord after = new PropertyKeyTokenRecord(before);
        after.initialize(true, 13);
        after.setCreated();

        byte[] bytes = new byte[] {
            5, 0, 0, 0, 42, 0, 0, 0, 0, 0, -1, -1, -1, -1, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 13, 0, 0, 0, 0
        };
        InMemoryClosableChannel channel = createChannel(bytes);

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
        PropertyKeyTokenRecord before = new PropertyKeyTokenRecord(42);
        PropertyKeyTokenRecord after = new PropertyKeyTokenRecord(before);
        after.initialize(true, 13);
        after.setCreated();
        after.setInternal(true);

        byte[] bytes = new byte[] {
            5, 0, 0, 0, 42, 0, 0, 0, 0, 0, -1, -1, -1, -1, 0, 0, 0, 0, 33, 0, 0, 0, 0, 0, 0, 0, 13, 0, 0, 0, 0
        };
        InMemoryClosableChannel channel = createChannel(bytes);

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
        LabelTokenRecord before = new LabelTokenRecord(42);
        LabelTokenRecord after = new LabelTokenRecord(before);
        after.initialize(true, 13);
        after.setCreated();

        byte[] bytes = new byte[] {8, 0, 0, 0, 42, 0, -1, -1, -1, -1, 0, 0, 0, 0, 1, 0, 0, 0, 13, 0, 0, 0, 0};
        InMemoryClosableChannel channel = createChannel(bytes);

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
        LabelTokenRecord before = new LabelTokenRecord(42);
        LabelTokenRecord after = new LabelTokenRecord(before);
        after.initialize(true, 13);
        after.setCreated();
        after.setInternal(true);

        byte[] bytes = new byte[] {8, 0, 0, 0, 42, 0, -1, -1, -1, -1, 0, 0, 0, 0, 33, 0, 0, 0, 13, 0, 0, 0, 0};
        InMemoryClosableChannel channel = createChannel(bytes);

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
        RelationshipTypeTokenRecord before = new RelationshipTypeTokenRecord(42);
        RelationshipTypeTokenRecord after = new RelationshipTypeTokenRecord(before);
        after.initialize(true, 13);
        after.setCreated();

        byte[] bytes = new byte[] {4, 0, 0, 0, 42, 0, -1, -1, -1, -1, 0, 0, 0, 0, 1, 0, 0, 0, 13, 0, 0, 0, 0};
        InMemoryClosableChannel channel = createChannel(bytes);

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
        RelationshipTypeTokenRecord before = new RelationshipTypeTokenRecord(42);
        RelationshipTypeTokenRecord after = new RelationshipTypeTokenRecord(before);
        after.initialize(true, 13);
        after.setCreated();
        after.setInternal(true);

        byte[] bytes = new byte[] {4, 0, 0, 0, 42, 0, -1, -1, -1, -1, 0, 0, 0, 0, 33, 0, 0, 0, 13, 0, 0, 0, 0};
        InMemoryClosableChannel channel = createChannel(bytes);

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
        RelationshipRecord before = new RelationshipRecord(42);
        before.setLinks(-1, -1, -1);
        RelationshipRecord after = new RelationshipRecord(42);
        after.initialize(true, 0, 1, 2, 3, 4, 5, 6, 7, true, true);
        after.setCreated();

        byte[] bytes = new byte[] {
            3, 0, 0, 0, 0, 0, 0, 0, 42, 0, -1, -1, -1, -1, 3, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 2, 0, 0, 0,
            3, 0, 0, 0, 0, 0, 0, 0, 4, 0, 0, 0, 0, 0, 0, 0, 5, 0, 0, 0, 0, 0, 0, 0, 6, 0, 0, 0, 0, 0, 0, 0, 7, 0, 0, 0,
            0, 0, 0, 0, 0, 3
        };
        InMemoryClosableChannel channel = createChannel(bytes);

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
        RelationshipRecord before = new RelationshipRecord(42);
        before.initialize(true, 0, 1, 2, 3, 4, 5, 6, 7, true, true);
        before.setSecondaryUnitIdOnLoad(47);
        RelationshipRecord after = new RelationshipRecord(42);
        after.initialize(true, 0, 1, 8, 3, 4, 5, 6, 7, true, true);

        byte[] bytes = new byte[] {
            3, 0, 0, 0, 0, 0, 0, 0, 42, 13, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 2, 0, 0, 0, 3, 0, 0, 0, 0, 0,
                    0, 0, 4, 0, 0, 0, 0, 0, 0,
            0, 5, 0, 0, 0, 0, 0, 0, 0, 6, 0, 0, 0, 0, 0, 0, 0, 7, 0, 0, 0, 0, 0, 0, 0, 0, 3, 0, 0, 0, 0, 0, 0, 0, 47, 1,
                    0, 0, 0, 0, 0, 0, 0, 1,
            0, 0, 0, 0, 0, 0, 0, 8, 0, 0, 0, 3, 0, 0, 0, 0, 0, 0, 0, 4, 0, 0, 0, 0, 0, 0, 0, 5, 0, 0, 0, 0, 0, 0, 0, 6,
                    0, 0, 0, 0, 0, 0, 0, 7,
            0, 0, 0, 0, 0, 0, 0, 0, 3
        };
        InMemoryClosableChannel channel = createChannel(bytes);

        CommandReader reader = createReader();
        StorageCommand command = reader.read(channel);
        assertTrue(command instanceof Command.RelationshipCommand);

        Command.RelationshipCommand relationshipCommand = (Command.RelationshipCommand) command;
        assertBeforeAndAfterEquals(relationshipCommand, before, after);
    }

    @Test
    void readRelationshipCommandWithNonRequiredSecondaryUnit() throws IOException {
        RelationshipRecord before = new RelationshipRecord(42);
        before.initialize(true, 0, 1, 2, 3, 4, 5, 6, 7, true, true);
        before.setSecondaryUnitIdOnLoad(52);
        RelationshipRecord after = new RelationshipRecord(42);
        after.initialize(true, 0, 1, 8, 3, 4, 5, 6, 7, true, true);

        byte[] bytes = new byte[] {
            3, 0, 0, 0, 0, 0, 0, 0, 42, 13, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 2, 0, 0, 0, 3, 0, 0, 0, 0, 0,
                    0, 0, 4, 0, 0, 0, 0, 0, 0,
            0, 5, 0, 0, 0, 0, 0, 0, 0, 6, 0, 0, 0, 0, 0, 0, 0, 7, 0, 0, 0, 0, 0, 0, 0, 0, 3, 0, 0, 0, 0, 0, 0, 0, 52, 1,
                    0, 0, 0, 0, 0, 0, 0, 1,
            0, 0, 0, 0, 0, 0, 0, 8, 0, 0, 0, 3, 0, 0, 0, 0, 0, 0, 0, 4, 0, 0, 0, 0, 0, 0, 0, 5, 0, 0, 0, 0, 0, 0, 0, 6,
                    0, 0, 0, 0, 0, 0, 0, 7,
            0, 0, 0, 0, 0, 0, 0, 0, 3
        };
        InMemoryClosableChannel channel = createChannel(bytes);

        CommandReader reader = createReader();
        StorageCommand command = reader.read(channel);
        assertTrue(command instanceof Command.RelationshipCommand);

        Command.RelationshipCommand relationshipCommand = (Command.RelationshipCommand) command;
        assertBeforeAndAfterEquals(relationshipCommand, before, after);
    }

    @Test
    void readRelationshipCommandWithFixedReferenceFormat() throws IOException {
        RelationshipRecord before = new RelationshipRecord(42);
        before.initialize(true, 0, 1, 2, 3, 4, 5, 6, 7, true, true);
        before.setUseFixedReferences(true);
        RelationshipRecord after = new RelationshipRecord(42);
        after.initialize(true, 0, 1, 8, 3, 4, 5, 6, 7, true, true);
        after.setUseFixedReferences(true);

        byte[] bytes = new byte[] {
            3, 0, 0, 0, 0, 0, 0, 0, 42, 17, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 2, 0, 0, 0, 3, 0, 0, 0, 0, 0,
                    0, 0, 4, 0, 0, 0, 0, 0, 0,
            0, 5, 0, 0, 0, 0, 0, 0, 0, 6, 0, 0, 0, 0, 0, 0, 0, 7, 0, 0, 0, 0, 0, 0, 0, 0, 3, 17, 0, 0, 0, 0, 0, 0, 0, 1,
                    0, 0, 0, 0, 0, 0, 0, 8,
            0, 0, 0, 3, 0, 0, 0, 0, 0, 0, 0, 4, 0, 0, 0, 0, 0, 0, 0, 5, 0, 0, 0, 0, 0, 0, 0, 6, 0, 0, 0, 0, 0, 0, 0, 7,
                    0, 0, 0, 0, 0, 0, 0, 0,
            3
        };
        InMemoryClosableChannel channel = createChannel(bytes);

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
        RelationshipGroupRecord before =
                new RelationshipGroupRecord(42).initialize(false, 3, NULL_REF, NULL_REF, NULL_REF, NULL_REF, NULL_REF);
        RelationshipGroupRecord after = new RelationshipGroupRecord(42).initialize(true, 3, 4, 5, 6, 7, 8);
        after.setCreated();

        byte[] bytes = new byte[] {
            9, 0, 0, 0, 0, 0, 0, 0, 42, 0, 0, 3, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
            -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, 1, 0, 3, 0, 0, 0, 0,
            0, 0, 0, 8, 0, 0, 0, 0, 0, 0, 0, 4, 0, 0, 0, 0, 0, 0, 0, 5, 0, 0, 0, 0, 0, 0, 0, 6, 0, 0, 0, 0, 0, 0, 0, 7
        };
        InMemoryClosableChannel channel = createChannel(bytes);

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
        RelationshipGroupRecord before =
                new RelationshipGroupRecord(42).initialize(false, 3, NULL_REF, NULL_REF, NULL_REF, NULL_REF, NULL_REF);
        RelationshipGroupRecord after = new RelationshipGroupRecord(42).initialize(true, 3, 4, 5, 6, 7, 8);
        after.setSecondaryUnitIdOnCreate(17);
        after.setCreated();

        byte[] bytes = new byte[] {
            9, 0, 0, 0, 0, 0, 0, 0, 42, 0, 0, 3, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
            -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, 13, 0, 3, 0, 0, 0,
            0, 0, 0, 0, 8, 0, 0, 0, 0, 0, 0, 0, 4, 0, 0, 0, 0, 0, 0, 0, 5, 0, 0, 0, 0, 0, 0, 0, 6, 0, 0, 0, 0, 0, 0, 0,
            7, 0, 0, 0, 0, 0, 0, 0, 17
        };
        InMemoryClosableChannel channel = createChannel(bytes);

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
        RelationshipGroupRecord before =
                new RelationshipGroupRecord(42).initialize(false, 3, NULL_REF, NULL_REF, NULL_REF, NULL_REF, NULL_REF);
        RelationshipGroupRecord after = new RelationshipGroupRecord(42).initialize(true, 3, 4, 5, 6, 7, 8);
        after.setSecondaryUnitIdOnCreate(17);
        after.setCreated();

        byte[] bytes = new byte[] {
            9, 0, 0, 0, 0, 0, 0, 0, 42, 0, 0, 3, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
            -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, 13, 0, 3, 0, 0, 0,
            0, 0, 0, 0, 8, 0, 0, 0, 0, 0, 0, 0, 4, 0, 0, 0, 0, 0, 0, 0, 5, 0, 0, 0, 0, 0, 0, 0, 6, 0, 0, 0, 0, 0, 0, 0,
            7, 0, 0, 0, 0, 0, 0, 0, 17
        };
        InMemoryClosableChannel channel = createChannel(bytes);

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
        RelationshipGroupRecord before =
                new RelationshipGroupRecord(42).initialize(false, 3, NULL_REF, NULL_REF, NULL_REF, NULL_REF, NULL_REF);
        before.setUseFixedReferences(true);
        RelationshipGroupRecord after = new RelationshipGroupRecord(42).initialize(true, 3, 4, 5, 6, 7, 8);
        after.setUseFixedReferences(true);
        after.setCreated();

        byte[] bytes = new byte[] {
            9, 0, 0, 0, 0, 0, 0, 0, 42, 16, 0, 3, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
            -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, 17, 0, 3, 0, 0,
            0, 0, 0, 0, 0, 8, 0, 0, 0, 0, 0, 0, 0, 4, 0, 0, 0, 0, 0, 0, 0, 5, 0, 0, 0, 0, 0, 0, 0, 6, 0, 0, 0, 0, 0, 0,
            0, 7
        };
        InMemoryClosableChannel channel = createChannel(bytes);

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
        RelationshipGroupRecord before =
                new RelationshipGroupRecord(42).initialize(false, 3, NULL_REF, NULL_REF, NULL_REF, NULL_REF, NULL_REF);
        RelationshipGroupRecord after =
                new RelationshipGroupRecord(42).initialize(true, (1 << Short.SIZE) + 10, 4, 5, 6, 7, 8);
        after.setCreated();

        byte[] bytes = new byte[] {
            21, 0, 0, 0, 0, 0, 0, 0, 42, 0, 0, 3, 0, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
            -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, 1, 0, 10, 1, 0,
            0, 0, 0, 0, 0, 0, 8, 0, 0, 0, 0, 0, 0, 0, 4, 0, 0, 0, 0, 0, 0, 0, 5, 0, 0, 0, 0, 0, 0, 0, 6, 0, 0, 0, 0, 0,
            0, 0, 7
        };
        InMemoryClosableChannel channel = createChannel(bytes);

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
        NodeRecord before = new NodeRecord(42).initialize(true, 99, false, 33, 66);
        NodeRecord after = new NodeRecord(42).initialize(true, 99, false, 33, 66);
        before.setUseFixedReferences(true);
        after.setUseFixedReferences(true);

        byte[] bytes = new byte[] {
            1, 0, 0, 0, 0, 0, 0, 0, 42, 17, 0, 0, 0, 0, 0, 0, 0, 0, 33, 0, 0, 0, 0, 0, 0, 0, 99, 0, 0, 0, 0, 0, 0, 0,
            66, 0, 0, 0, 0, 17, 0, 0, 0, 0, 0, 0, 0, 0, 33, 0, 0, 0, 0, 0, 0, 0, 99, 0, 0, 0, 0, 0, 0, 0, 66, 0, 0, 0, 0
        };
        InMemoryClosableChannel channel = createChannel(bytes);

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
    void readPropertyCommandWithSecondaryUnit() throws IOException {
        PropertyRecord before = new PropertyRecord(1);
        PropertyRecord after = new PropertyRecord(1);
        after.setSecondaryUnitIdOnCreate(78);

        byte[] bytes = new byte[] {
            2, 0, 0, 0, 0, 0, 0, 0, 1, 0, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
                    -1, -1, -1, -1, -1, 0, 0,
            0, 0, 0, 12, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
                    0, 0, 0, 0, 0, 0, 0, 78,
            0, 0, 0, 0, 0
        };
        InMemoryClosableChannel channel = createChannel(bytes);

        CommandReader reader = createReader();
        StorageCommand command = reader.read(channel);
        assertTrue(command instanceof Command.PropertyCommand);

        Command.PropertyCommand propertyCommand = (Command.PropertyCommand) command;

        // Then
        assertBeforeAndAfterEquals(propertyCommand, before, after);
    }

    @Test
    void readPropertyCommandWithNonRequiredSecondaryUnit() throws IOException {
        PropertyRecord before = new PropertyRecord(1);
        PropertyRecord after = new PropertyRecord(1);
        after.setSecondaryUnitIdOnCreate(78);

        byte[] bytes = new byte[] {
            2, 0, 0, 0, 0, 0, 0, 0, 1, 0, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
                    -1, -1, -1, -1, -1, 0, 0,
            0, 0, 0, 12, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
                    0, 0, 0, 0, 0, 0, 0, 78,
            0, 0, 0, 0, 0
        };
        InMemoryClosableChannel channel = createChannel(bytes);

        CommandReader reader = createReader();
        StorageCommand command = reader.read(channel);
        assertTrue(command instanceof Command.PropertyCommand);

        Command.PropertyCommand propertyCommand = (Command.PropertyCommand) command;

        // Then
        assertBeforeAndAfterEquals(propertyCommand, before, after);
    }

    @Test
    void readPropertyCommandWithFixedReferenceFormat() throws IOException {
        PropertyRecord before = new PropertyRecord(1);
        PropertyRecord after = new PropertyRecord(1);
        before.setUseFixedReferences(true);
        after.setUseFixedReferences(true);

        byte[] bytes = new byte[] {
            2, 0, 0, 0, 0, 0, 0, 0, 1, 16, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
            -1, -1, -1, -1, -1, 0, 0, 0, 0, 0, 16, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
            -1, -1, -1, -1, -1, -1, -1, 0, 0, 0, 0, 0
        };
        InMemoryClosableChannel channel = createChannel(bytes);

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
        CommandReader reader = createReader();

        byte[] bytes = new byte[] {
            1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 3, 0, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
            -1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 3, 0, -1, -1, -1, -1, -1,
            -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 4, 0, 0, 0, 0, 0, -1, -1,
            -1, -1, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, -1, -1, -1, -1, 35, 0, 0, 0, 10, -1,
            -1, -1, -1, -1, -1, -1, -1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 3, 0, 0, 0, 0, 0, 0, 0, 0, 0, -1, -1, -1, -1, 3,
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, -1, -1, -1, -1, -1, -1,
            -1, -1, 0, 0, 0, 0, 0, 0, 0, 1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, 3, 5, 0, 0,
            0, 0, 0, 0, 0, 0, 0, -1, -1, -1, -1, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0,
            0, -1, -1, -1, -1, 35, 0, 0, 0, 10, -1, -1, -1, -1, -1, -1, -1, -1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 2, 0, 0,
            0, 0, 0, 0, 0, 0, 0, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
            -1, -1, 0, 0, 0, 0, 0, 1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
            -1, -1, -1, -1, 1, 8, 0, 0, 0, 7, -75, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0
        };
        InMemoryClosableChannel channel = createChannel(bytes);

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
        SchemaRecord before = new SchemaRecord(42);
        SchemaRecord after = new SchemaRecord(before);
        after.initialize(true, 353);
        after.setConstraint(true);
        after.setCreated();

        byte[] bytes = new byte[] {
            18, 0, 0, 0, 0, 0, 0, 0, 42, 1, 0, 3, 1, 0, 0, 0, 0, 0, 0, 1, 97, 0, 0, 0, 10, 0, 0, 0, 39, 95, 95, 111,
            114, 103, 46, 110, 101, 111, 52, 106, 46, 83, 99, 104, 101, 109, 97, 82, 117, 108, 101, 46, 115, 99, 104,
            101, 109, 97, 69, 110, 116, 105, 116, 121, 84, 121, 112, 101, 9, 0, 0, 0, 4, 78, 79, 68, 69, 0, 0, 0, 27,
            95, 95, 111, 114, 103, 46, 110, 101, 111, 52, 106, 46, 83, 99, 104, 101, 109, 97, 82, 117, 108, 101, 46,
            110, 97, 109, 101, 9, 0, 0, 0, 8, 105, 110, 100, 101, 120, 95, 52, 50, 0, 0, 0, 47, 95, 95, 111, 114, 103,
            46, 110, 101, 111, 52, 106, 46, 83, 99, 104, 101, 109, 97, 82, 117, 108, 101, 46, 115, 99, 104, 101, 109,
            97, 80, 114, 111, 112, 101, 114, 116, 121, 83, 99, 104, 101, 109, 97, 84, 121, 112, 101, 9, 0, 0, 0, 19, 67,
            79, 77, 80, 76, 69, 84, 69, 95, 65, 76, 76, 95, 84, 79, 75, 69, 78, 83, 0, 0, 0, 40, 95, 95, 111, 114, 103,
            46, 110, 101, 111, 52, 106, 46, 83, 99, 104, 101, 109, 97, 82, 117, 108, 101, 46, 105, 110, 100, 101, 120,
            80, 114, 111, 118, 105, 100, 101, 114, 78, 97, 109, 101, 9, 0, 0, 0, 9, 85, 110, 100, 101, 99, 105, 100,
            101, 100, 0, 0, 0, 43, 95, 95, 111, 114, 103, 46, 110, 101, 111, 52, 106, 46, 83, 99, 104, 101, 109, 97, 82,
            117, 108, 101, 46, 105, 110, 100, 101, 120, 80, 114, 111, 118, 105, 100, 101, 114, 86, 101, 114, 115, 105,
            111, 110, 9, 0, 0, 0, 1, 48, 0, 0, 0, 40, 95, 95, 111, 114, 103, 46, 110, 101, 111, 52, 106, 46, 83, 99,
            104, 101, 109, 97, 82, 117, 108, 101, 46, 115, 99, 104, 101, 109, 97, 80, 114, 111, 112, 101, 114, 116, 121,
            73, 100, 115, 11, 0, 0, 0, 2, 5, 0, 0, 0, 2, 0, 0, 0, 3, 0, 0, 0, 37, 95, 95, 111, 114, 103, 46, 110, 101,
            111, 52, 106, 46, 83, 99, 104, 101, 109, 97, 82, 117, 108, 101, 46, 115, 99, 104, 101, 109, 97, 82, 117,
            108, 101, 84, 121, 112, 101, 9, 0, 0, 0, 5, 73, 78, 68, 69, 88, 0, 0, 0, 32, 95, 95, 111, 114, 103, 46, 110,
            101, 111, 52, 106, 46, 83, 99, 104, 101, 109, 97, 82, 117, 108, 101, 46, 105, 110, 100, 101, 120, 84, 121,
            112, 101, 9, 0, 0, 0, 5, 82, 65, 78, 71, 69, 0, 0, 0, 36, 95, 95, 111, 114, 103, 46, 110, 101, 111, 52, 106,
            46, 83, 99, 104, 101, 109, 97, 82, 117, 108, 101, 46, 105, 110, 100, 101, 120, 82, 117, 108, 101, 84, 121,
            112, 101, 9, 0, 0, 0, 10, 78, 79, 78, 95, 85, 78, 73, 81, 85, 69, 0, 0, 0, 38, 95, 95, 111, 114, 103, 46,
            110, 101, 111, 52, 106, 46, 83, 99, 104, 101, 109, 97, 82, 117, 108, 101, 46, 115, 99, 104, 101, 109, 97,
            69, 110, 116, 105, 116, 121, 73, 100, 115, 11, 0, 0, 0, 1, 5, 0, 0, 0, 1
        };
        InMemoryClosableChannel channel = createChannel(bytes);

        CommandReader reader = createReader();
        Command.SchemaRuleCommand command = (Command.SchemaRuleCommand) reader.read(channel);

        assertBeforeAndAfterEquals(command, before, after);
    }

    private InMemoryClosableChannel createChannel(byte[] bytes) {
        // 4.2 transaction log commands are big-endian
        return new InMemoryClosableChannel(bytes, true, true, ByteOrder.BIG_ENDIAN);
    }

    protected CommandReader createReader() {
        return LogCommandSerializationV4_2.INSTANCE;
    }

    static <RECORD extends AbstractBaseRecord> void assertBeforeAndAfterEquals(
            Command.BaseCommand<RECORD> command, RECORD before, RECORD after) {
        assertEqualsIncludingFlags(before, command.getBefore());
        assertEqualsIncludingFlags(after, command.getAfter());
    }

    private static <RECORD extends AbstractBaseRecord> void assertEqualsIncludingFlags(RECORD expected, RECORD record) {
        assertThat(expected).isEqualTo(record);
        assertThat(expected.isCreated()).as("Created flag mismatch").isEqualTo(record.isCreated());
        assertThat(expected.isUseFixedReferences())
                .as("Fixed references flag mismatch")
                .isEqualTo(record.isUseFixedReferences());
        assertThat(expected.isSecondaryUnitCreated())
                .as("Secondary unit created flag mismatch")
                .isEqualTo(record.isSecondaryUnitCreated());
    }
}
