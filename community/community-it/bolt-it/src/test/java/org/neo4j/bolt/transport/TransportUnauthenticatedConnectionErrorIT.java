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
package org.neo4j.bolt.transport;

import static org.neo4j.bolt.testing.assertions.BoltConnectionAssertions.assertThat;
import static org.neo4j.bolt.testing.messages.BoltDefaultWire.hello;
import static org.neo4j.configuration.connectors.BoltConnector.EncryptionLevel.OPTIONAL;

import io.netty.buffer.Unpooled;
import java.io.IOException;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Consumer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.neo4j.bolt.AbstractBoltTransportsTest;
import org.neo4j.bolt.negotiation.ProtocolVersion;
import org.neo4j.bolt.testing.client.TransportConnection;
import org.neo4j.bolt.testing.messages.BoltV40Wire;
import org.neo4j.configuration.connectors.BoltConnector;
import org.neo4j.configuration.connectors.BoltConnectorInternalSettings;
import org.neo4j.graphdb.config.Setting;
import org.neo4j.io.ByteUnit;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.packstream.io.PackstreamBuf;
import org.neo4j.packstream.struct.StructHeader;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.OtherThread;
import org.neo4j.test.extension.OtherThreadExtension;
import org.neo4j.test.extension.testdirectory.EphemeralTestDirectoryExtension;

@EphemeralTestDirectoryExtension
@Neo4jWithSocketExtension
@ExtendWith(OtherThreadExtension.class)
public class TransportUnauthenticatedConnectionErrorIT extends AbstractBoltTransportsTest {
    @Inject
    private Neo4jWithSocket server;

    @Inject
    private OtherThread otherThread;

    @BeforeEach
    public void setup(TestInfo testInfo) throws IOException {
        server.setConfigure(getSettingsFunction());
        server.init(testInfo);
        address = server.lookupDefaultConnector();
    }

    @Override
    protected Consumer<Map<Setting<?>, Object>> getSettingsFunction() {
        return settings -> {
            settings.put(BoltConnector.encryption_level, OPTIONAL);
            settings.put(
                    BoltConnectorInternalSettings.unsupported_bolt_unauth_connection_timeout, Duration.ofSeconds(5));
            settings.put(
                    BoltConnectorInternalSettings.unsupported_bolt_unauth_connection_max_inbound_bytes,
                    ByteUnit.kibiBytes(1));
        };
    }

    @ParameterizedTest(name = "{displayName} {arguments}")
    @MethodSource("argumentsProvider")
    public void shouldFinishHelloMessage(TransportConnection.Factory connectionFactory) throws Exception {
        initParameters(connectionFactory);

        // When
        connection.connect().sendDefaultProtocolVersion().send(hello());

        // Then
        assertThat(connection).negotiatesDefaultVersion().receivesSuccess();
    }

    @ParameterizedTest(name = "{displayName} {arguments}")
    @MethodSource("argumentsProvider")
    public void shouldTimeoutTooSlowConnection(TransportConnection.Factory connectionFactory) throws Exception {
        initParameters(connectionFactory);

        // Given
        var handshakeBytes = Unpooled.buffer()
                .writeInt(0x6060B017)
                .writeInt(TransportConnection.DEFAULT_PROTOCOL_VERSION.encode())
                .writeInt(ProtocolVersion.INVALID.encode())
                .writeInt(ProtocolVersion.INVALID.encode())
                .writeInt(ProtocolVersion.INVALID.encode());

        // When
        connection.connect();

        otherThread.execute(() -> {
            while (handshakeBytes.isReadable()) {
                connection.sendRaw(handshakeBytes.readSlice(1));
                Thread.sleep(500);
            }

            return null;
        });

        // Then
        assertThat(connection).isEventuallyTerminated();
    }

    @ParameterizedTest(name = "{displayName} {arguments}")
    @MethodSource("argumentsProvider")
    public void shouldTimeoutToHandshakeForHalfHandshake(TransportConnection.Factory connectionFactory)
            throws Exception {
        initParameters(connectionFactory);

        // Given half written bolt handshake message
        var buf = Unpooled.buffer().writeInt(0x6060B017);

        // When
        connection.connect().sendRaw(buf);

        // Then
        assertThat(connection).isEventuallyTerminated();
    }

    @ParameterizedTest(name = "{displayName} {arguments}")
    @MethodSource("argumentsProvider")
    public void shouldTimeoutToAuthForHalfHelloMessage(TransportConnection.Factory connectionFactory) throws Exception {
        initParameters(connectionFactory);

        // Given half written hello message
        var msg = hello();
        var buffer = msg.readSlice(msg.readableBytes() / 2);

        // When
        connection.connect().sendDefaultProtocolVersion().send(buffer);

        // Then
        assertThat(connection).negotiatesDefaultVersion().isEventuallyTerminated();
    }

    @ParameterizedTest(name = "{displayName} {arguments}")
    @MethodSource("argumentsProvider")
    public void shouldCloseConnectionDueToTooBigHelloMessage(TransportConnection.Factory connectionFactory)
            throws Exception {
        initParameters(connectionFactory);

        // When
        var meta = new HashMap<String, Object>();
        for (int i = 0; i < 100; i++) {
            meta.put("index-" + i, i);
        }

        connection.connect().sendDefaultProtocolVersion().send(hello(meta, null));

        // Then
        assertThat(connection).negotiatesDefaultVersion().isEventuallyTerminated();
    }

    @ParameterizedTest(name = "{displayName} {arguments}")
    @MethodSource("argumentsProvider")
    public void shouldCloseConnectionDueToTooBigDeclaredMapInHelloMessage(TransportConnection.Factory connectionFactory)
            throws Exception {
        initParameters(connectionFactory);

        // When
        connection
                .connect()
                .sendDefaultProtocolVersion()
                .send(PackstreamBuf.allocUnpooled()
                        .writeStructHeader(new StructHeader(1, BoltV40Wire.MESSAGE_TAG_HELLO))
                        .writeMapHeader(Integer.MAX_VALUE)
                        .writeString("foo")
                        .writeString("bar"));

        // Then
        assertThat(connection)
                .negotiatesDefaultVersion()
                .receivesFailureFuzzy(
                        Status.Request.Invalid,
                        "Illegal value for field \"extra\": Value of size 2147483647 exceeded limit of")
                .isEventuallyTerminated();
    }

    @ParameterizedTest(name = "{displayName} {arguments}")
    @MethodSource("argumentsProvider")
    public void shouldCloseConnectionDueToTooBigDeclaredListInHelloMessage(
            TransportConnection.Factory connectionFactory) throws Exception {
        initParameters(connectionFactory);

        // When
        connection
                .connect()
                .sendDefaultProtocolVersion()
                .send(PackstreamBuf.allocUnpooled()
                        .writeStructHeader(new StructHeader(1, BoltV40Wire.MESSAGE_TAG_HELLO))
                        .writeMapHeader(1)
                        .writeString("x")
                        .writeListHeader(Integer.MAX_VALUE)
                        .writeString("foo")
                        .writeString("bar"));

        // Then
        assertThat(connection)
                .negotiatesDefaultVersion()
                .receivesFailureFuzzy(
                        Status.Request.Invalid,
                        "Illegal value for field \"extra\": Value of size 2147483647 exceeded limit of")
                .isEventuallyTerminated();
    }
}
