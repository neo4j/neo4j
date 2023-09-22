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
package org.neo4j.storageengine.api.enrichment;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import org.junit.jupiter.api.Test;
import org.neo4j.internal.kernel.api.connectioninfo.ClientConnectionInfo;
import org.neo4j.internal.kernel.api.security.AuthSubject;
import org.neo4j.internal.kernel.api.security.SecurityContext;
import org.neo4j.io.fs.BufferBackedChannel;

class TxMetadataTest {
    @Test
    void roundTripShouldSucceed() throws IOException {
        // given
        final var serverId = "some.server";
        final var metadata = TxMetadata.create(CaptureMode.DIFF, serverId, securityContext(), 42L);

        // when
        try (var channel = new BufferBackedChannel(1024)) {
            metadata.serialize(channel);

            channel.flip();
            final var metadata2 = TxMetadata.deserialize(channel);

            // then
            assertThat(metadata2.lastCommittedTx()).isEqualTo(metadata.lastCommittedTx());
            assertThat(metadata2.captureMode()).isEqualTo(metadata.captureMode());
            assertThat(metadata2.serverId()).isEqualTo(metadata.serverId());
            assertThat(metadata2.subject().authenticatedUser())
                    .isEqualTo(metadata.subject().authenticatedUser());
            assertThat(metadata2.subject().executingUser())
                    .isEqualTo(metadata.subject().executingUser());
            assertThat(metadata2.connectionInfo().protocol())
                    .isEqualTo(metadata.connectionInfo().protocol());
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
        when(authSubject.authenticatedUser()).thenReturn("rod");
        when(authSubject.executingUser()).thenReturn("jane");
        return authSubject;
    }
}
