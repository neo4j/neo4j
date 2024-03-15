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

import static java.util.Objects.requireNonNull;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import org.neo4j.internal.kernel.api.connectioninfo.ClientConnectionInfo;
import org.neo4j.internal.kernel.api.security.AuthSubject;
import org.neo4j.internal.kernel.api.security.AuthenticationResult;
import org.neo4j.internal.kernel.api.security.SecurityContext;
import org.neo4j.io.fs.ReadableChannel;
import org.neo4j.io.fs.WritableChannel;
import org.neo4j.string.Mask;

/**
 * Captures the transaction metadata when the {@link EnrichmentMode} is in an active state
 */
public record TxMetadata(
        CaptureMode captureMode,
        String serverId,
        AuthSubject subject,
        ClientConnectionInfo connectionInfo,
        long lastCommittedTx)
        implements Mask.Maskable {

    public TxMetadata(
            CaptureMode captureMode,
            String serverId,
            AuthSubject subject,
            ClientConnectionInfo connectionInfo,
            long lastCommittedTx) {
        this.captureMode = requireNonNull(captureMode, "captureMode must not be null");
        this.serverId = requireNonNull(serverId, "serverId must not be null");
        this.subject = requireNonNull(subject, "subject must not be null");
        this.connectionInfo = requireNonNull(connectionInfo, "connectionInfo must not be null");
        this.lastCommittedTx = lastCommittedTx;
    }

    /**
     * Create the metadata based on the information of the provided transaction
     *
     * @param captureMode the mode describing the contents of the associated {@link Enrichment}
     * @param serverId the server ID of the machine the transaction executed on
     * @param securityContext the transaction being enriched
     * @param lastCommittedTx the last committed transaction ID
     * @return the metadata for the provided transaction
     */
    public static TxMetadata create(
            CaptureMode captureMode, String serverId, SecurityContext securityContext, long lastCommittedTx) {
        return new TxMetadata(
                captureMode, serverId, securityContext.subject(), securityContext.connectionInfo(), lastCommittedTx);
    }

    /**
     * @param channel the channel containing the enrichment metadata
     * @return a metadata object that describes the enrichment of a previous transaction
     * @throws IOException if unable to read the metadata
     */
    public static TxMetadata deserialize(ReadableChannel channel) throws IOException {
        final var lastCommittedTx = channel.getLong();
        final var captureMode = readEnum(channel.get());
        final var serverId = readString(channel);
        final var subject = readSubject(channel);
        final var connectionInfo = readConnectionInfo(channel);

        return new TxMetadata(captureMode, serverId, subject, connectionInfo, lastCommittedTx);
    }

    /**
     * @param channel the channel to write the enrichment data to
     * @throws IOException if unable to write the enrichment data
     */
    @SuppressWarnings("deprecation")
    public void serialize(WritableChannel channel) throws IOException {
        channel.putLong(lastCommittedTx);
        channel.put(captureMode.id());
        serialize(channel, serverId);
        serialize(channel, subject.authenticatedUser());
        serialize(channel, subject.executingUser());
        serialize(channel, connectionInfo.asConnectionDetails());
        serialize(channel, connectionInfo.protocol());
        serialize(channel, connectionInfo.connectionId());
        serialize(channel, connectionInfo.clientAddress());
        serialize(channel, connectionInfo.requestURI());
    }

    private static CaptureMode readEnum(byte flag) {
        return CaptureMode.BY_ID.get(flag);
    }

    private static AuthSubject readSubject(ReadableChannel channel) throws IOException {
        final var authenticatedUser = readString(channel);
        final var executingUser = requireNonNull(readString(channel));
        return new AuthSubject() {
            @Override
            public AuthenticationResult getAuthenticationResult() {
                return AuthenticationResult.SUCCESS;
            }

            @Override
            public boolean hasUsername(String username) {
                return executingUser.equals(username);
            }

            @Override
            public String executingUser() {
                return executingUser;
            }

            @Override
            public String authenticatedUser() {
                return authenticatedUser == null ? executingUser : authenticatedUser;
            }
        };
    }

    private static ClientConnectionInfo readConnectionInfo(ReadableChannel channel) throws IOException {
        final var details = readString(channel);
        final var protocol = readString(channel);
        final var connectionId = readString(channel);
        final var clientAddress = readString(channel);
        final var requestUri = readString(channel);
        return new ClientConnectionInfo() {
            @Override
            public String asConnectionDetails() {
                return details;
            }

            @Override
            public String protocol() {
                return protocol;
            }

            @Override
            public String connectionId() {
                return connectionId;
            }

            @Override
            public String clientAddress() {
                return clientAddress;
            }

            @Override
            public String requestURI() {
                return requestUri;
            }
        };
    }

    private static String readString(ReadableChannel channel) throws IOException {
        final var length = channel.getInt();
        if (length < 0) {
            return null;
        }

        final var bytes = new byte[length];
        channel.get(bytes, bytes.length);
        return new String(bytes, StandardCharsets.UTF_8);
    }

    private static void serialize(WritableChannel channel, String data) throws IOException {
        if (data == null) {
            channel.putInt(-1);
        } else {
            final var bytes = data.getBytes(StandardCharsets.UTF_8);
            channel.putInt(bytes.length).put(bytes, bytes.length);
        }
    }

    @Override
    public String toString(Mask mask) {
        return "TxMetadata(%s, %s, %s, %s, %d)"
                .formatted(
                        mask.filter(captureMode),
                        mask.filter(serverId),
                        mask.filter(subject),
                        mask.filter(connectionInfo),
                        lastCommittedTx);
    }
}
