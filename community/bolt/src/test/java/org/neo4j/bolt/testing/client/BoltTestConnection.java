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
package org.neo4j.bolt.testing.client;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelOption;
import java.io.IOException;
import java.net.SocketAddress;
import java.security.PrivateKey;
import java.security.cert.X509Certificate;
import java.util.Set;
import java.util.stream.Stream;
import org.neo4j.bolt.negotiation.ProtocolVersion;
import org.neo4j.bolt.negotiation.message.ProtocolCapability;
import org.neo4j.bolt.testing.client.error.BoltTestClientException;
import org.neo4j.bolt.testing.client.struct.ProtocolProposal;
import org.neo4j.bolt.testing.messages.BoltDefaultWire;
import org.neo4j.packstream.io.PackstreamBuf;

public interface BoltTestConnection extends AutoCloseable {

    /**
     * Defines the default protocol version which is transmitted when no specific value is passed.
     * <p>
     * This value should be updated along with {@link BoltDefaultWire} in order to transmit the correct message variations.
     */
    ProtocolVersion DEFAULT_PROTOCOL_VERSION = new ProtocolVersion(4, 4);

    /**
     * Retrieves a stream of factories capable of constructing connections for all default transports supported by Bolt.
     *
     * @return a stream of connection factories.
     */
    static Stream<Factory> factories() {
        return Stream.of(
                SocketConnection.factory(),
                SecureSocketConnection.factory(),
                WebSocketConnection.factory(),
                SecureWebSocketConnection.factory());
    }

    /**
     * Establishes a connection to the desired host if none has already been established.
     *
     * @return a reference to this connection.
     * @throws IOException when establishing the connection fails.
     */
    BoltTestConnection connect();

    /**
     * Sets the certificate and private key to use for the purposes of client authentication.
     * <p />
     * This method takes effect when the connection is established (e.g. requires an uninitialized
     * connection).
     *
     * @param certificate a certificate.
     * @param privateKey a private key.
     * @return a reference to this connection.
     */
    BoltTestConnection setCertificate(X509Certificate certificate, PrivateKey privateKey);

    /**
     * Sets a given socket option (if supported).
     * <p>
     * When the underlying transport does not support socket options, the call is simply ignored to permit sharing of code between different transport tests.
     *
     * @param option an option.
     * @param value  an option value.
     * @param <T>    an option type.
     * @return a reference to this connection.
     */
    <T> BoltTestConnection setOption(ChannelOption<T> option, T value);

    /**
     * Terminates the connection.
     *
     * @return a reference to this connection.
     * @throws IOException when an error is reported during the disconnect.
     */
    BoltTestConnection disconnect();

    /**
     * Re-establishes a connection to the desired host.
     * <p>
     * If the connection is currently in-tact, it will be terminated first before establishing a new connection. If the connection has previously been
     * terminated, a new connection is established.
     *
     * @return a reference to this connection.
     * @throws IOException when establishing the connection fails.
     */
    default BoltTestConnection reconnect() {
        try {
            this.disconnect();
        } catch (BoltTestClientException ignore) {
        }

        return this.connect();
    }

    /**
     * Transmits a given payload via this connection.
     *
     * @param buf an arbitrary payload.
     * @return a reference to this connection.
     * @throws IOException when transmitting the payload fails.
     */
    BoltTestConnection sendRaw(ByteBuf buf);

    /**
     * Transmits a given payload via this connection.
     *
     * @param rawBytes an arbitrary payload.
     * @return a reference to this connection.
     * @throws IOException when transmitting the payload fails.
     */
    default BoltTestConnection sendRaw(byte[] rawBytes) {
        return this.sendRaw(Unpooled.wrappedBuffer(rawBytes));
    }

    default BoltTestConnection sendDefaultProtocolVersion() {
        return this.send(DEFAULT_PROTOCOL_VERSION);
    }

    default BoltTestConnection send(ProtocolVersion version) {
        return this.send(version, ProtocolVersion.INVALID, ProtocolVersion.INVALID, ProtocolVersion.INVALID);
    }

    default BoltTestConnection send(ProtocolVersion version1, ProtocolVersion version2) {
        return this.send(version1, version2, ProtocolVersion.INVALID, ProtocolVersion.INVALID);
    }

    default BoltTestConnection send(ProtocolVersion version1, ProtocolVersion version2, ProtocolVersion version3) {
        return this.send(version1, version2, version3, ProtocolVersion.INVALID);
    }

    BoltTestConnection send(
            ProtocolVersion version1, ProtocolVersion version2, ProtocolVersion version3, ProtocolVersion version4);

    BoltTestConnection send(ProtocolVersion version, Set<ProtocolCapability> capabilities) throws IOException;

    /**
     * Transmits a chunked message via this connection.
     *
     * @param buf an arbitrary payload.
     * @return a reference to this connection.
     * @throws IOException when transmitting the payload fails.
     */
    BoltTestConnection send(ByteBuf buf);

    /**
     * Transmits a chunked message via this connection.
     *
     * @param buf an arbitrary payload.
     * @return a reference to this connection.
     * @throws IOException when transmitting the payload fails.
     */
    default BoltTestConnection send(PackstreamBuf buf) {
        return this.send(buf.getTarget());
    }

    /**
     * Retrieves the total amount of NOOPs received during the last message read operation.
     * <p>
     * Applies to {@link #receiveMessage()}.
     *
     * @return a number of NOOP chunks.
     */
    long noopCount();

    /**
     * Retrieves a payload of the desired length from this connection.
     *
     * @param length a payload length in bytes.
     * @return a reference to this connection.
     * @throws IOException          when transmitting the payload fails.
     * @when the thread is interrupted while waiting for additional data to arrive.
     */
    ByteBuf receive(int length);

    ProtocolVersion receiveNegotiatedVersion();

    ProtocolProposal receiveProtocolProposal();

    int receiveChunkHeader();

    int receiveVarInt() throws IOException, InterruptedException;

    ByteBuf receiveMessage();

    boolean isClosed();

    @Override
    default void close() {
        try {
            this.disconnect();
        } catch (BoltTestClientException ignore) {
        }
    }

    @FunctionalInterface
    interface Factory {
        BoltTestConnection create(SocketAddress address);
    }
}
