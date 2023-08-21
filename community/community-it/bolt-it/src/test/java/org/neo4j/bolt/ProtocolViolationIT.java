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
package org.neo4j.bolt;

import static org.neo4j.values.storable.Values.longValue;
import static org.neo4j.values.storable.Values.stringValue;

import java.io.IOException;
import java.util.List;
import java.util.function.Consumer;
import org.neo4j.bolt.protocol.io.StructType;
import org.neo4j.bolt.test.annotation.BoltTestExtension;
import org.neo4j.bolt.test.annotation.connection.initializer.Authenticated;
import org.neo4j.bolt.test.annotation.test.ProtocolTest;
import org.neo4j.bolt.testing.assertions.BoltConnectionAssertions;
import org.neo4j.bolt.testing.client.TransportConnection;
import org.neo4j.bolt.testing.messages.BoltV40Wire;
import org.neo4j.bolt.testing.messages.BoltWire;
import org.neo4j.bolt.transport.Neo4jWithSocketExtension;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.packstream.io.PackstreamBuf;
import org.neo4j.packstream.struct.StructHeader;
import org.neo4j.test.extension.testdirectory.EphemeralTestDirectoryExtension;
import org.neo4j.values.virtual.MapValueBuilder;

@EphemeralTestDirectoryExtension
@Neo4jWithSocketExtension
@BoltTestExtension
public class ProtocolViolationIT {

    private static void sendRun(TransportConnection connection, Consumer<PackstreamBuf> packer) throws IOException {
        var buf = PackstreamBuf.allocUnpooled()
                .writeStructHeader(new StructHeader(3, BoltV40Wire.MESSAGE_TAG_RUN))
                .writeString("RETURN $x") // statement
                .writeMapHeader(1) // parameters
                .writeString("x");

        packer.accept(buf);

        connection.send(buf.writeMapHeader(0) // extra
                .getTarget());
    }

    @ProtocolTest
    void shouldFailWhenNullKeyIsSent(@Authenticated TransportConnection connection) throws IOException {
        sendRun(connection, buf -> buf.writeMapHeader(1).writeNull().writeString("foo"));

        BoltConnectionAssertions.assertThat(connection)
                .receivesFailure(
                        Status.Request.Invalid,
                        "Illegal value for field \"params\": Unexpected type: Expected STRING but got NONE");
    }

    @ProtocolTest
    void shouldFailWhenDuplicateKeyIsSent(@Authenticated TransportConnection connection) throws IOException {
        sendRun(connection, buf -> buf.writeMapHeader(2)
                .writeString("foo")
                .writeString("bar")
                .writeString("foo")
                .writeString("changed_my_mind"));

        BoltConnectionAssertions.assertThat(connection)
                .receivesFailure(
                        Status.Request.Invalid, "Illegal value for field \"params\": Duplicate map key: \"foo\"");
    }

    @ProtocolTest
    void shouldFailWhenNodeIsSentWithRun(BoltWire wire, @Authenticated TransportConnection connection)
            throws IOException {
        var properties = new MapValueBuilder();
        properties.add("the_answer", longValue(42));
        properties.add("one_does_not_simply", stringValue("break_decoding"));

        sendRun(connection, buf -> wire.nodeValue(buf, "42", 42, List.of("Broken", "Dreams")));

        BoltConnectionAssertions.assertThat(connection)
                .receivesFailure(
                        Status.Request.Invalid, "Illegal value for field \"params\": Unexpected struct tag: 0x4E");
    }

    @ProtocolTest
    void shouldFailWhenRelationshipIsSentWithRun(BoltWire wire, @Authenticated TransportConnection connection)
            throws IOException {
        var properties = new MapValueBuilder();
        properties.add("the_answer", longValue(42));
        properties.add("one_does_not_simply", stringValue("break_decoding"));

        sendRun(connection, buf -> wire.relationshipValue(buf, "42", 42, "21", 21, "84", 84, "RUINS_EXPECTATIONS"));

        BoltConnectionAssertions.assertThat(connection)
                .receivesFailure(
                        Status.Request.Invalid, "Illegal value for field \"params\": Unexpected struct tag: 0x52");
    }

    @ProtocolTest
    void shouldFailWhenPathIsSentWithRun(BoltWire wire, @Authenticated TransportConnection connection)
            throws IOException {
        sendRun(connection, buf -> {
            buf.writeStructHeader(new StructHeader(3, StructType.PATH.getTag()));

            buf.writeListHeader(2);
            wire.nodeValue(buf, "42", 42, List.of("Computer"));
            wire.nodeValue(buf, "84", 84, List.of("Vendor"));

            buf.writeListHeader(1);
            wire.unboundRelationshipValue(buf, "13", 13, "MAKES");

            buf.writeListHeader(2);
            buf.writeInt(1);
            buf.writeInt(1);
        });

        BoltConnectionAssertions.assertThat(connection)
                .receivesFailure(
                        Status.Request.Invalid, "Illegal value for field \"params\": Unexpected struct tag: 0x50");

        connection.send(wire.reset());

        BoltConnectionAssertions.assertThat(connection).receivesSuccess();
    }

    @ProtocolTest
    void shouldTerminateConnectionWhenUnknownMessageIsSent(@Authenticated TransportConnection connection)
            throws IOException {
        connection.send(PackstreamBuf.allocUnpooled()
                .writeStructHeader(new StructHeader(1, (short) 0x42))
                .writeListHeader(1)
                .writeInt(42));

        BoltConnectionAssertions.assertThat(connection).isEventuallyTerminated();
    }
}
