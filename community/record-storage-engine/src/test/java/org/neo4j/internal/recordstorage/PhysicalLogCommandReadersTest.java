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
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.neo4j.internal.recordstorage.Command.RelationshipGroupCommand;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.neo4j.io.fs.ReadableChannel;
import org.neo4j.kernel.impl.store.record.Record;
import org.neo4j.kernel.impl.store.record.RelationshipGroupRecord;
import org.neo4j.storageengine.api.CommandReader;
import org.neo4j.storageengine.api.StorageCommand;

class PhysicalLogCommandReadersTest {
    private static final long ID = 42;
    private static final byte IN_USE_FLAG = Record.IN_USE.byteValue();
    private static final int TYPE_AS_INT = Short.MAX_VALUE + 42;
    private static final long NEXT = 43;
    private static final long FIRST_OUT = 44;
    private static final long FIRST_IN = 45;
    private static final long FIRST_LOOP = 46;
    private static final long OWNING_NODE = 47;

    @ParameterizedTest
    @ValueSource(classes = {LogCommandSerializationV4_2.class, LogCommandSerializationV4_3_D3.class})
    void readRelGroupWithHugeTypeBefore5_0(Class<CommandReader> readerClass)
            throws IOException, IllegalAccessException, InstantiationException, NoSuchMethodException,
                    InvocationTargetException {
        CommandReader reader = readerClass.getDeclaredConstructor().newInstance();
        StorageCommand command = reader.read(channelWithExtendedRelGroupRecordBefore5_0());
        assertValidRelGroupCommand(command);
    }

    @Test
    void readRelGroupWithHugeType() throws IOException {
        StorageCommand command = LogCommandSerializationV5_0.INSTANCE.read(channelWithRelGroupRecord());
        assertValidRelGroupCommand(command);
    }

    private static void assertValidRelGroupCommand(StorageCommand command) {
        assertThat(command).isInstanceOf(RelationshipGroupCommand.class);
        RelationshipGroupCommand relGroupCommand = (RelationshipGroupCommand) command;
        RelationshipGroupRecord record = relGroupCommand.getAfter();

        assertEquals(ID, record.getId());
        if (IN_USE_FLAG == Record.IN_USE.byteValue()) {
            Assertions.assertTrue(record.inUse());
        } else if (IN_USE_FLAG == Record.NOT_IN_USE.byteValue()) {
            Assertions.assertFalse(record.inUse());
        } else {
            throw new IllegalStateException("Illegal inUse flag: " + IN_USE_FLAG);
        }
        assertEquals(TYPE_AS_INT, record.getType());
        assertEquals(NEXT, record.getNext());
        assertEquals(FIRST_OUT, record.getFirstOut());
        assertEquals(FIRST_IN, record.getFirstIn());
        assertEquals(FIRST_LOOP, record.getFirstLoop());
        assertEquals(OWNING_NODE, record.getOwningNode());
    }

    private static ReadableChannel channelWithExtendedRelGroupRecordBefore5_0() throws IOException {
        ReadableChannel channel = mock(ReadableChannel.class);

        // Mock for both before and after state
        when(channel.get())
                .thenReturn(NeoCommandType.REL_GROUP_EXTENDED_COMMAND)
                .thenReturn(IN_USE_FLAG)
                .thenReturn((byte) (TYPE_AS_INT >>> Short.SIZE))
                .thenReturn(IN_USE_FLAG)
                .thenReturn((byte) (TYPE_AS_INT >>> Short.SIZE));
        when(channel.getLong())
                .thenReturn(ID)
                .thenReturn(NEXT)
                .thenReturn(FIRST_OUT)
                .thenReturn(FIRST_IN)
                .thenReturn(FIRST_LOOP)
                .thenReturn(OWNING_NODE)
                .thenReturn(NEXT)
                .thenReturn(FIRST_OUT)
                .thenReturn(FIRST_IN)
                .thenReturn(FIRST_LOOP)
                .thenReturn(OWNING_NODE);
        when(channel.getShort()).thenReturn((short) TYPE_AS_INT);

        return channel;
    }

    private static ReadableChannel channelWithRelGroupRecord() throws IOException {
        ReadableChannel channel = mock(ReadableChannel.class);

        // Mock for both before and after state
        when(channel.get()).thenReturn(NeoCommandType.REL_GROUP_COMMAND).thenReturn(IN_USE_FLAG);
        when(channel.getLong())
                .thenReturn(ID)
                .thenReturn(NEXT)
                .thenReturn(FIRST_OUT)
                .thenReturn(FIRST_IN)
                .thenReturn(FIRST_LOOP)
                .thenReturn(OWNING_NODE)
                .thenReturn(NEXT)
                .thenReturn(FIRST_OUT)
                .thenReturn(FIRST_IN)
                .thenReturn(FIRST_LOOP)
                .thenReturn(OWNING_NODE);
        when(channel.getInt()).thenReturn(TYPE_AS_INT);

        return channel;
    }
}
