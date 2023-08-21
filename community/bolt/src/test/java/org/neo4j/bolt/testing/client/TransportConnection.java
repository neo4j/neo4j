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
import java.io.IOException;
import java.net.SocketAddress;
import java.net.SocketOption;
import java.util.stream.Stream;
import org.neo4j.bolt.negotiation.ProtocolVersion;
import org.neo4j.bolt.testing.client.tls.SecureSocketConnection;
import org.neo4j.bolt.testing.client.websocket.SecureWebSocketConnection;
import org.neo4j.bolt.testing.client.websocket.WebSocketConnection;
import org.neo4j.bolt.testing.messages.BoltDefaultWire;
import org.neo4j.packstream.io.PackstreamBuf;

public interface TransportConnection extends AutoCloseable {

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
    TransportConnection connect() throws IOException;

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
    default <T> TransportConnection setOption(SocketOption<T> option, T value) {
        return this;
    }

    /**
     * Terminates the connection.
     *
     * @return a reference to this connection.
     * @throws IOException when an error is reported during the disconnect.
     */
    TransportConnection disconnect() throws IOException;

    /**
     * Re-establishes a connection to the desired host.
     * <p>
     * If the connection is currently in-tact, it will be terminated first before establishing a new connection. If the connection has previously been
     * terminated, a new connection is established.
     *
     * @return a reference to this connection.
     * @throws IOException when establishing the connection fails.
     */
    default TransportConnection reconnect() throws IOException {
        try {
            this.disconnect();
        } catch (IOException ignore) {
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
    TransportConnection sendRaw(ByteBuf buf) throws IOException;

    /**
     * Transmits a given payload via this connection.
     *
     * @param rawBytes an arbitrary payload.
     * @return a reference to this connection.
     * @throws IOException when transmitting the payload fails.
     */
    default TransportConnection sendRaw(byte[] rawBytes) throws IOException {
        return this.sendRaw(Unpooled.wrappedBuffer(rawBytes));
    }

    default TransportConnection sendDefaultProtocolVersion() throws IOException {
        return this.send(DEFAULT_PROTOCOL_VERSION);
    }

    TransportConnection send(ProtocolVersion version) throws IOException;

    TransportConnection send(ProtocolVersion version1, ProtocolVersion version2) throws IOException;

    TransportConnection send(ProtocolVersion version1, ProtocolVersion version2, ProtocolVersion version3)
            throws IOException;

    TransportConnection send(
            ProtocolVersion version1, ProtocolVersion version2, ProtocolVersion version3, ProtocolVersion version4)
            throws IOException;

    /**
     * Transmits a chunked message via this connection.
     *
     * @param buf an arbitrary payload.
     * @return a reference to this connection.
     * @throws IOException when transmitting the payload fails.
     */
    TransportConnection send(ByteBuf buf) throws IOException;

    /**
     * Transmits a chunked message via this connection.
     *
     * @param buf an arbitrary payload.
     * @return a reference to this connection.
     * @throws IOException when transmitting the payload fails.
     */
    default TransportConnection send(PackstreamBuf buf) throws IOException {
        return this.send(buf.getTarget());
    }

    /**
     * Retrieves the total amount of NOOPs received during the last message read operation.
     * <p>
     * Applies to {@link #receiveMessage()} and{@link #receiveMessage(int)}.
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
     * @throws InterruptedException when the thread is interrupted while waiting for additional data to arrive.
     */
    ByteBuf receive(int length) throws IOException, InterruptedException;

    ProtocolVersion receiveNegotiatedVersion() throws IOException, InterruptedException;

    int receiveChunkHeader() throws IOException, InterruptedException;

    ByteBuf receiveMessage() throws IOException, InterruptedException;

    default boolean isClosed() throws InterruptedException {
        try {
            this.sendRaw(new byte[] {0, 0}).receive(1);
            return false;
        } catch (IOException e) {
            return true;
        }
    }

    @Override
    default void close() throws Exception {
        try {
            this.disconnect();
        } catch (IOException ignore) {
        }
    }

    @FunctionalInterface
    interface Factory {
        TransportConnection create(SocketAddress address);
    }
}
