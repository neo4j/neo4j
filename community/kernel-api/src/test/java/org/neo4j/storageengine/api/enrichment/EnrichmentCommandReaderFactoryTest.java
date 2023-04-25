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
import java.util.Optional;
import org.junit.jupiter.api.Test;
import org.neo4j.internal.kernel.api.connectioninfo.ClientConnectionInfo;
import org.neo4j.internal.kernel.api.security.AuthSubject;
import org.neo4j.internal.kernel.api.security.SecurityContext;
import org.neo4j.io.fs.ReadableChannel;
import org.neo4j.io.fs.WritableChannel;
import org.neo4j.kernel.KernelVersion;
import org.neo4j.storageengine.api.BaseCommandReader;
import org.neo4j.storageengine.api.CommandReader;
import org.neo4j.storageengine.api.CommandReaderFactory;
import org.neo4j.storageengine.api.StorageCommand;

public class EnrichmentCommandReaderFactoryTest {

    private static final int BUFFER_SIZE = 1024;

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

    @Test
    void factoryCanReadEnrichmentCommand() throws IOException {
        try (var channel = new ChannelBuffer(BUFFER_SIZE)) {
            zeroPad(channel);

            final var metadata = metadata();

            channel.put(EnrichmentCommand.COMMAND_CODE);
            metadata.serialize(channel);
            channel.flip();

            final var reader = reader();
            final var readCommand = reader.read(channel);

            assertThat(readCommand).isInstanceOf(TestEnrichmentCommand.class);
            assertMetadataEquals(metadata, ((TestEnrichmentCommand) readCommand).metadata);
            assertThat(((TestEnrichmentCommand) readCommand).mode).isEqualTo(ReadMode.FULLY);
        }
    }

    @Test
    void factoryCanReadMixedCommand() throws IOException {
        try (var channel = new ChannelBuffer(BUFFER_SIZE)) {
            zeroPad(channel);

            final var command1 = new TestCommand(42);
            final var command2 = new TestCommand(43);
            final var command3 = new TestCommand(44);

            final var metadata = metadata();

            command1.serialize(channel);
            command2.serialize(channel);
            channel.put(EnrichmentCommand.COMMAND_CODE);
            metadata.serialize(channel);
            command3.serialize(channel);
            channel.flip();

            final var reader = reader();
            assertThat(reader.read(channel)).isEqualTo(command1);
            assertThat(reader.read(channel)).isEqualTo(command2);

            final var readCommand = reader.read(channel);
            assertThat(readCommand).isInstanceOf(TestEnrichmentCommand.class);
            assertMetadataEquals(metadata, ((TestEnrichmentCommand) readCommand).metadata);
            assertThat(((TestEnrichmentCommand) readCommand).mode)
                    .as("Should return the 'fully read' command rather than the 'skipped' one from base reader")
                    .isEqualTo(ReadMode.FULLY);

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
            return new TestEnrichmentCommand(enrichment.metadata, ReadMode.FULLY);
        });

        final var readerFactory = new EnrichmentCommandReaderFactory(new TestCommandReaderFactory(), commandFactory);
        return readerFactory.get(KernelVersion.VERSION_CDC_INTRODUCED);
    }

    private static TxMetadata metadata() {
        return TxMetadata.create(CaptureMode.FULL, "some.server", securityContext(), 42L);
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
                        return new TestEnrichmentCommand(metadata, ReadMode.SKIPPED);
                    }

                    throw new IllegalArgumentException("Invalid commandType: " + commandType);
                }
            };
        }
    }

    private enum ReadMode {
        SKIPPED,
        FULLY
    }

    private record TestEnrichmentCommand(TxMetadata metadata, ReadMode mode) implements EnrichmentCommand {

        @Override
        public KernelVersion kernelVersion() {
            return KernelVersion.VERSION_CDC_INTRODUCED;
        }

        @Override
        public void serialize(WritableChannel channel) throws IOException {
            channel.put(COMMAND_CODE);
            metadata.serialize(channel);
        }

        @Override
        public Optional<Enrichment> enrichment() {
            return Optional.empty();
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
