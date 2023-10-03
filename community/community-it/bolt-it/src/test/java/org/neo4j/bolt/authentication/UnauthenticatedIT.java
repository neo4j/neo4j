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
package org.neo4j.bolt.authentication;

import java.io.IOException;
import java.time.Duration;
import java.util.Map;
import org.junit.jupiter.api.extension.ExtendWith;
import org.neo4j.bolt.test.annotation.BoltTestExtension;
import org.neo4j.bolt.test.annotation.connection.initializer.VersionSelected;
import org.neo4j.bolt.test.annotation.setup.SettingsFunction;
import org.neo4j.bolt.test.annotation.test.TransportTest;
import org.neo4j.bolt.testing.assertions.BoltConnectionAssertions;
import org.neo4j.bolt.testing.client.TransportConnection;
import org.neo4j.bolt.testing.messages.BoltV40Wire;
import org.neo4j.bolt.testing.messages.BoltWire;
import org.neo4j.bolt.transport.Neo4jWithSocketExtension;
import org.neo4j.configuration.connectors.BoltConnectorInternalSettings;
import org.neo4j.graphdb.config.Setting;
import org.neo4j.io.ByteUnit;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.packstream.io.PackstreamBuf;
import org.neo4j.packstream.struct.StructHeader;
import org.neo4j.test.extension.OtherThreadExtension;
import org.neo4j.test.extension.testdirectory.EphemeralTestDirectoryExtension;

@EphemeralTestDirectoryExtension
@Neo4jWithSocketExtension
@BoltTestExtension
@ExtendWith(OtherThreadExtension.class)
public class UnauthenticatedIT {

    @SettingsFunction
    static void customizeSettings(Map<Setting<?>, Object> settings) {
        settings.put(BoltConnectorInternalSettings.unsupported_bolt_unauth_connection_timeout, Duration.ofSeconds(5));
        settings.put(
                BoltConnectorInternalSettings.unsupported_bolt_unauth_connection_max_inbound_bytes,
                ByteUnit.kibiBytes(1));
    }

    @TransportTest
    void shouldTimeoutWhenTruncatedHelloIsReceived(BoltWire wire, @VersionSelected TransportConnection connection)
            throws IOException {
        var msg = wire.hello();
        var buffer = msg.readSlice(msg.readableBytes() / 2);

        // When
        connection.send(buffer);

        // Then
        BoltConnectionAssertions.assertThat(connection).isEventuallyTerminated();
    }

    @TransportTest
    void shouldTerminateConnectionWhenLargeHelloIsReceived(
            BoltWire wire, @VersionSelected TransportConnection connection) throws IOException {

        connection.send(wire.hello(x -> {
            for (int i = 0; i < 200; i++) {
                x.withBadKeyPair("index-" + i, i);
            }
            return x;
        }));

        // Then
        BoltConnectionAssertions.assertThat(connection).isEventuallyTerminated();
    }

    @TransportTest
    void shouldTerminateConnectionWhenLargeDeclaredMetaMapIsReceived(@VersionSelected TransportConnection connection)
            throws IOException {
        connection.send(PackstreamBuf.allocUnpooled()
                .writeStructHeader(new StructHeader(1, BoltV40Wire.MESSAGE_TAG_HELLO))
                .writeMapHeader(Integer.MAX_VALUE)
                .writeString("foo")
                .writeString("bar"));

        BoltConnectionAssertions.assertThat(connection)
                .receivesFailureFuzzy(
                        Status.Request.Invalid,
                        "Illegal value for field \"extra\": Value of size 2147483647 exceeded limit of")
                .isEventuallyTerminated();
    }

    @TransportTest
    void shouldTerminateConnectionWhenLargeDeclaredListParameterIsReceived(
            @VersionSelected TransportConnection connection) throws IOException {
        connection.send(PackstreamBuf.allocUnpooled()
                .writeStructHeader(new StructHeader(1, BoltV40Wire.MESSAGE_TAG_HELLO))
                .writeMapHeader(1)
                .writeString("x")
                .writeListHeader(Integer.MAX_VALUE)
                .writeString("foo")
                .writeString("bar"));

        // Then
        BoltConnectionAssertions.assertThat(connection)
                .receivesFailureFuzzy(
                        Status.Request.Invalid,
                        "Illegal value for field \"extra\": Value of size 2147483647 exceeded limit of")
                .isEventuallyTerminated();
    }
}
