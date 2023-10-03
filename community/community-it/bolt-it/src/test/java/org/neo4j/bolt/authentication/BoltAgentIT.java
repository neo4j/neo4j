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
import java.util.Map;
import org.junit.jupiter.api.extension.ExtendWith;
import org.neo4j.bolt.test.annotation.BoltTestExtension;
import org.neo4j.bolt.test.annotation.connection.initializer.VersionSelected;
import org.neo4j.bolt.test.annotation.test.ProtocolTest;
import org.neo4j.bolt.test.annotation.wire.selector.ExcludeWire;
import org.neo4j.bolt.testing.annotation.Version;
import org.neo4j.bolt.testing.assertions.BoltConnectionAssertions;
import org.neo4j.bolt.testing.client.TransportConnection;
import org.neo4j.bolt.testing.messages.AbstractBoltWire;
import org.neo4j.bolt.testing.messages.BoltWire;
import org.neo4j.bolt.transport.Neo4jWithSocketExtension;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.packstream.io.PackstreamBuf;
import org.neo4j.packstream.struct.StructHeader;
import org.neo4j.test.extension.OtherThreadExtension;
import org.neo4j.test.extension.testdirectory.EphemeralTestDirectoryExtension;

@EphemeralTestDirectoryExtension
@Neo4jWithSocketExtension
@BoltTestExtension
@ExtendWith(OtherThreadExtension.class)
@ExcludeWire({@Version(major = 4), @Version(major = 5, minor = 2, range = 2)})
public class BoltAgentIT {
    @ProtocolTest
    void shouldSucceedWhenAddingExtraValues(BoltWire wire, @VersionSelected TransportConnection connection)
            throws IOException {
        connection.send(wire.hello(x -> x.withBoltAgent(Map.of("product", "test-agent", "extra", "included"))));
        BoltConnectionAssertions.assertThat(connection).receivesSuccess();
    }

    @ProtocolTest
    void shouldFailWhenBoltAgentIsOmitted(@VersionSelected TransportConnection connection) throws IOException {
        connection.send(PackstreamBuf.allocUnpooled()
                .writeStructHeader(new StructHeader(1, AbstractBoltWire.MESSAGE_TAG_HELLO))
                .writeMap(Map.of("scheme", "none", "user_agent", "ignore"))
                .getTarget());

        BoltConnectionAssertions.assertThat(connection)
                .receivesFailure(
                        Status.Request.Invalid,
                        "Illegal value for field \"bolt_agent\": Must be a map with string keys and string values.");
    }

    @ProtocolTest
    void shouldFailWhenInvalidBoltAgentIsGiven(BoltWire wire, @VersionSelected TransportConnection connection)
            throws IOException {
        connection.send(wire.hello(x -> x.withScheme("none").withBadBoltAgent("42L")));

        BoltConnectionAssertions.assertThat(connection)
                .receivesFailure(
                        Status.Request.Invalid,
                        "Illegal value for field \"bolt_agent\": Must be a map with string keys and string values.");
    }

    @ProtocolTest
    void shouldFailWhenBoltAgentInvalidValues(BoltWire wire, @VersionSelected TransportConnection connection)
            throws IOException {
        connection.send(wire.hello(x -> x.withScheme("none").withBadBoltAgent(Map.of("product", 1))));

        BoltConnectionAssertions.assertThat(connection)
                .receivesFailure(
                        Status.Request.Invalid,
                        "Illegal value for field \"bolt_agent\": Must be a map with string keys and string values.");
    }

    @ProtocolTest
    void shouldFailWhenBoltAgentMissingProduct(BoltWire wire, @VersionSelected TransportConnection connection)
            throws IOException {
        connection.send(wire.hello(x -> x.withScheme("none").withBadBoltAgent(Map.of("invalid", "value"))));

        BoltConnectionAssertions.assertThat(connection)
                .receivesFailure(
                        Status.Request.Invalid,
                        "Illegal value for field \"bolt_agent\": Expected map to contain key: 'product'.");
    }
}
