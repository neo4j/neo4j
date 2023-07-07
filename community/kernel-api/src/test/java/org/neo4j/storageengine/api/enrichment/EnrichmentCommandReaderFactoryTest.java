/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
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
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.storageengine.api.enrichment;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.nio.ByteBuffer;
import org.junit.jupiter.api.Test;
import org.neo4j.internal.kernel.api.connectioninfo.ClientConnectionInfo;
import org.neo4j.internal.kernel.api.security.AuthSubject;
import org.neo4j.internal.kernel.api.security.SecurityContext;
import org.neo4j.io.fs.ReadableChannel;
import org.neo4j.io.fs.WritableChannel;
import org.neo4j.kernel.KernelVersion;
import org.neo4j.memory.EmptyMemoryTracker;
import org.neo4j.storageengine.api.BaseCommandReader;
import org.neo4j.storageengine.api.CommandReader;
import org.neo4j.storageengine.api.CommandReaderFactory;
import org.neo4j.storageengine.api.StorageCommand;

public class EnrichmentCommandReaderFactoryTest {

    private static final int BUFFER_SIZE = 1024;

    private static final long[] ENTITY_DATA = new long[] {1, 2, 3};
    private static final long[] DETAILS_DATA = new long[] {11, 12, 13};
    private static final long[] CHANGES_DATA = new long[] {111, 112, 113};
    private static final long[] VALUES_DATA = new long[] {1111, 1112, 1113};

    @Test
    void factoryCanReadDelegatesCommands() throws IOException {
        try (var channel = new ChannelBuffer(BUFFER_SIZE)) {
            zeroPad(channel);

            final var command = new TestCommand(42);
            command.serialize(channel);
            channel.flip();

            final var reader = reader();
            assertThat(reader.read(channel)).isEqualTo(command);
        }
    }

    @SuppressWarnings("resource")
    @Test
    void factoryCanReadEnrichmentCommand() throws IOException {
        try (var channel = new ChannelBuffer(BUFFER_SIZE)) {
            zeroPad(channel);

            final var enrichment = enrichment();

            channel.put(EnrichmentCommand.COMMAND_CODE);
            enrichment.serialize(channel);
            channel.flip();

            final var reader = reader();
            final var readCommand = reader.read(channel);

            assertThat(readCommand).isInstanceOf(TestEnrichmentCommand.class);
            assertMetadataEquals(enrichment.metadata(), ((TestEnrichmentCommand) readCommand).metadata);

            var readEnrichment = EnrichmentCommand.extractForReading((TestEnrichmentCommand) readCommand);
            assertContents(readEnrichment.entities(), ENTITY_DATA);
            assertContents(readEnrichment.entityDetails(), DETAILS_DATA);
            assertContents(readEnrichment.entityChanges(), CHANGES_DATA);
            assertContents(readEnrichment.values(), VALUES_DATA);
        }
    }

    @SuppressWarnings("resource")
    @Test
    void factoryCanReadMixedCommand() throws IOException {
        try (var channel = new ChannelBuffer(BUFFER_SIZE)) {
            zeroPad(channel);

            final var command1 = new TestCommand(42);
            final var command2 = new TestCommand(43);
            final var command3 = new TestCommand(44);

            final var enrichment = enrichment();

            command1.serialize(channel);
            command2.serialize(channel);
            channel.put(EnrichmentCommand.COMMAND_CODE);
            enrichment.serialize(channel);
            command3.serialize(channel);
            channel.flip();

            final var reader = reader();
            assertThat(reader.read(channel)).isEqualTo(command1);
            assertThat(reader.read(channel)).isEqualTo(command2);

            final var readCommand = reader.read(channel);
            assertThat(readCommand).isInstanceOf(TestEnrichmentCommand.class);
            assertMetadataEquals(enrichment.metadata(), ((TestEnrichmentCommand) readCommand).metadata);

            var readEnrichment = EnrichmentCommand.extractForReading((TestEnrichmentCommand) readCommand);
            assertContents(readEnrichment.entities(), ENTITY_DATA);
            assertContents(readEnrichment.entityDetails(), DETAILS_DATA);
            assertContents(readEnrichment.entityChanges(), CHANGES_DATA);
            assertContents(readEnrichment.values(), VALUES_DATA);

            assertThat(reader.read(channel)).isEqualTo(command3);
        }
    }

    private static void assertMetadataEquals(TxMetadata expected, TxMetadata actual) {
        assertThat(actual.lastCommittedTx()).isEqualTo(expected.lastCommittedTx());
        assertThat(actual.captureMode()).isEqualTo(expected.captureMode());
        assertThat(actual.serverId()).isEqualTo(expected.serverId());
        assertThat(actual.subject().executingUser())
                .isEqualTo(expected.subject().executingUser());
        assertThat(actual.connectionInfo().protocol())
                .isEqualTo(expected.connectionInfo().protocol());
    }

    private static void zeroPad(WritableChannel channel) throws IOException {
        channel.put((byte) 0).put((byte) 0).put((byte) 0);
    }

    private static CommandReader reader() {
        final var commandFactory = mock(EnrichmentCommandFactory.class);
        when(commandFactory.create(any(), any())).thenAnswer(answer -> {
            final var enrichment = answer.<Enrichment>getArgument(1);
            return new TestEnrichmentCommand(enrichment.metadata, enrichment);
        });

        final var readerFactory = new EnrichmentCommandReaderFactory(
                new TestCommandReaderFactory(), commandFactory, () -> EmptyMemoryTracker.INSTANCE);
        return readerFactory.get(KernelVersion.VERSION_CDC_INTRODUCED);
    }

    private static Enrichment.Write enrichment() {
        final var entities = new WriteEnrichmentChannel(EmptyMemoryTracker.INSTANCE);
        for (var entity : ENTITY_DATA) {
            entities.putLong(entity);
        }

        final var details = new WriteEnrichmentChannel(EmptyMemoryTracker.INSTANCE);
        for (var entity : DETAILS_DATA) {
            details.putLong(entity);
        }

        final var changes = new WriteEnrichmentChannel(EmptyMemoryTracker.INSTANCE);
        for (var change : CHANGES_DATA) {
            changes.putLong(change);
        }

        final var values = new WriteEnrichmentChannel(EmptyMemoryTracker.INSTANCE);
        for (var change : VALUES_DATA) {
            values.putLong(change);
        }

        return new Enrichment.Write(
                TxMetadata.create(CaptureMode.FULL, "some.server", securityContext(), 42L),
                entities.flip(),
                details.flip(),
                changes.flip(),
                values.flip());
    }

    private static void assertContents(ByteBuffer buffer, long[] values) {
        for (var value : values) {
            assertThat(buffer.getLong()).isEqualTo(value);
        }
    }

    private static SecurityContext securityContext() {
        final var subject = subject();
        final var securityContext = mock(SecurityContext.class);
        when(securityContext.subject()).thenReturn(subject);
        when(securityContext.connectionInfo()).thenReturn(ClientConnectionInfo.EMBEDDED_CONNECTION);
        return securityContext;
    }

    private static AuthSubject subject() {
        final var authSubject = mock(AuthSubject.class);
        when(authSubject.executingUser()).thenReturn("me");
        return authSubject;
    }

    private static class TestCommandReaderFactory implements CommandReaderFactory {
        @Override
        public CommandReader get(KernelVersion version) {
            return new BaseCommandReader() {
                @Override
                public StorageCommand read(byte commandType, ReadableChannel channel) throws IOException {
                    if (commandType == TestCommand.TYPE) {
                        return new TestCommand(channel.getLong());
                    } else if (commandType == EnrichmentCommand.COMMAND_CODE) {
                        final var metadata = TxMetadata.deserialize(channel);
                        readPastData(channel);
                        return new TestEnrichmentCommand(metadata, null);
                    }

                    throw new IllegalArgumentException("Invalid commandType: " + commandType);
                }

                @Override
                public KernelVersion kernelVersion() {
                    return version;
                }

                private void readPastData(ReadableChannel channel) throws IOException {
                    final var longsCount = channel.getInt() + channel.getInt() + channel.getInt() + channel.getInt();
                    for (var i = 0; i < longsCount; i++) {
                        channel.getLong(); // drop the value
                    }
                }
            };
        }
    }

    private record TestEnrichmentCommand(TxMetadata metadata, Enrichment enrichment) implements EnrichmentCommand {

        @Override
        public KernelVersion kernelVersion() {
            return KernelVersion.VERSION_CDC_INTRODUCED;
        }

        @Override
        public void serialize(WritableChannel channel) throws IOException {
            channel.put(COMMAND_CODE);
            enrichment.serialize(channel);
        }

        @Override
        public Enrichment enrichment() {
            return enrichment;
        }
    }

    private record TestCommand(long id) implements StorageCommand {

        private static final byte TYPE = 1;

        @Override
        public void serialize(WritableChannel channel) throws IOException {
            channel.put(TYPE).putLong(id);
        }

        @Override
        public KernelVersion kernelVersion() {
            return KernelVersion.VERSION_CDC_INTRODUCED;
        }
    }
}
