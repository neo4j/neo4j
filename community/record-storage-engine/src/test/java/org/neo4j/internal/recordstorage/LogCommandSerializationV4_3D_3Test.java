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

import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.nio.ByteOrder;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.neo4j.kernel.impl.store.record.MetaDataRecord;
import org.neo4j.kernel.impl.store.record.RelationshipGroupRecord;
import org.neo4j.kernel.impl.transaction.log.InMemoryClosableChannel;
import org.neo4j.storageengine.api.CommandReader;
import org.neo4j.storageengine.api.StorageCommand;
import org.neo4j.test.extension.RandomExtension;

@ExtendWith(RandomExtension.class)
class LogCommandSerializationV4_3D_3Test extends LogCommandSerializationV4_2Test {
    @Test
    void shouldReadAndWriteMetaDataCommand() throws IOException {
        // Given
        MetaDataRecord before = new MetaDataRecord();
        MetaDataRecord after = new MetaDataRecord();
        after.initialize(true, 999);

        byte[] bytes = new byte[] {
            19, -1, -1, -1, -1, -1, -1, -1, -1, 0, -1, -1, -1, -1, -1, -1, -1, -1, 1, 0, 0, 0, 0, 0, 0, 3, -25
        };
        InMemoryClosableChannel channel = createChannel(bytes);

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
        RelationshipGroupRecord before =
                new RelationshipGroupRecord(42).initialize(false, 3, NULL_REF, NULL_REF, NULL_REF, NULL_REF, NULL_REF);
        RelationshipGroupRecord after = new RelationshipGroupRecord(42).initialize(true, 3, 4, 5, 6, 7, 8);
        after.setHasExternalDegreesOut(true);
        after.setHasExternalDegreesIn(false);
        after.setHasExternalDegreesLoop(true);
        after.setCreated();

        byte[] bytes = new byte[] {
            9, 0, 0, 0, 0, 0, 0, 0, 42, 0, 0, 3, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
            -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -95, 0, 3, 0, 0, 0,
            0, 0, 0, 0, 8, 0, 0, 0, 0, 0, 0, 0, 4, 0, 0, 0, 0, 0, 0, 0, 5, 0, 0, 0, 0, 0, 0, 0, 6, 0, 0, 0, 0, 0, 0, 0,
            7
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
    void shouldReadRelationshipGroupExtendedCommandIncludingExternalDegrees() throws Throwable {
        // Given
        RelationshipGroupRecord before =
                new RelationshipGroupRecord(42).initialize(false, 3, NULL_REF, NULL_REF, NULL_REF, NULL_REF, NULL_REF);
        RelationshipGroupRecord after =
                new RelationshipGroupRecord(42).initialize(true, (1 << Short.SIZE) + 10, 4, 5, 6, 7, 8);
        after.setHasExternalDegreesOut(false);
        after.setHasExternalDegreesIn(true);
        after.setHasExternalDegreesLoop(false);
        after.setCreated();

        byte[] bytes = new byte[] {
            21, 0, 0, 0, 0, 0, 0, 0, 42, 0, 0, 3, 0, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
            -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, 65, 0, 10, 1, 0,
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

    private InMemoryClosableChannel createChannel(byte[] bytes) {
        // 4.3 transaction log commands are big-endian
        return new InMemoryClosableChannel(bytes, true, true, ByteOrder.BIG_ENDIAN);
    }

    @Override
    protected CommandReader createReader() {
        return LogCommandSerializationV4_3_D3.INSTANCE;
    }
}
