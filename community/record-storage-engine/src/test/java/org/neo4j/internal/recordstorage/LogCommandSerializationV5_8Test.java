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
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import org.junit.jupiter.api.Test;
import org.neo4j.internal.kernel.api.connectioninfo.ClientConnectionInfo;
import org.neo4j.internal.kernel.api.security.AuthSubject;
import org.neo4j.internal.kernel.api.security.SecurityContext;
import org.neo4j.internal.recordstorage.Command.RecordEnrichmentCommand;
import org.neo4j.kernel.impl.transaction.log.InMemoryClosableChannel;
import org.neo4j.memory.EmptyMemoryTracker;
import org.neo4j.storageengine.api.enrichment.CaptureMode;
import org.neo4j.storageengine.api.enrichment.Enrichment;
import org.neo4j.storageengine.api.enrichment.TxMetadata;
import org.neo4j.storageengine.api.enrichment.WriteEnrichmentChannel;

public class LogCommandSerializationV5_8Test {

    private static final long[] DUMMY_DATA = new long[] {1, 2, 3};

    @Test
    void enrichmentSupported() throws IOException {
        final var metadata = metadata();
        try (var channel = new InMemoryClosableChannel()) {
            final var serialization = LogCommandSerializationV5_8.INSTANCE;

            final var enrichment = new Enrichment.Write(metadata(), dummyData(), dummyData(), dummyData(), dummyData());

            final var writer = channel.writer();
            writer.beginChecksumForWriting();
            serialization.writeEnrichmentCommand(writer, new RecordEnrichmentCommand(serialization, enrichment));
            final var afterEnrichment = writer.getCurrentLogPosition();
            writer.putChecksum();

            final var command = serialization.read(channel);
            assertThat(command).isInstanceOf(RecordEnrichmentCommand.class);
            assertMetadata(
                    metadata, ((RecordEnrichmentCommand) command).enrichment().metadata());
            assertThat(channel.reader().getCurrentLogPosition())
                    .as("should have read the metadata and past the dummy data blocks")
                    .isEqualTo(afterEnrichment);
        }
    }

    static void assertMetadata(TxMetadata expected, TxMetadata actual) {
        assertThat(expected.lastCommittedTx()).isEqualTo(actual.lastCommittedTx());
        assertThat(expected.captureMode()).isEqualTo(actual.captureMode());
        assertThat(expected.serverId()).isEqualTo(actual.serverId());
        assertThat(expected.subject().authenticatedUser())
                .isEqualTo(actual.subject().authenticatedUser());
        assertThat(expected.subject().executingUser())
                .isEqualTo(actual.subject().executingUser());
        assertThat(expected.connectionInfo().protocol())
                .isEqualTo(actual.connectionInfo().protocol());
    }

    private static WriteEnrichmentChannel dummyData() {
        final var dataChannel = new WriteEnrichmentChannel(EmptyMemoryTracker.INSTANCE);
        for (var entity : DUMMY_DATA) {
            dataChannel.putLong(entity);
        }
        return dataChannel.flip();
    }

    static TxMetadata metadata() {
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
        when(authSubject.authenticatedUser()).thenReturn("me");
        when(authSubject.executingUser()).thenReturn("me");
        return authSubject;
    }
}
