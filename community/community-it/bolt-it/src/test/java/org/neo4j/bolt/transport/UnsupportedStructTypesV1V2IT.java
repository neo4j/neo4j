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
import static org.neo4j.bolt.testing.messages.BoltDefaultWire.reset;
import static org.neo4j.packstream.testing.example.Paths.ALL_PATHS;
import static org.neo4j.values.storable.Values.longValue;
import static org.neo4j.values.storable.Values.stringValue;

import java.io.IOException;
import java.util.function.Consumer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.neo4j.bolt.AbstractBoltTransportsTest;
import org.neo4j.bolt.protocol.io.LegacyBoltValueWriter;
import org.neo4j.bolt.testing.client.TransportConnection;
import org.neo4j.bolt.testing.messages.BoltV40Wire;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.packstream.io.PackstreamBuf;
import org.neo4j.packstream.struct.StructHeader;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.testdirectory.EphemeralTestDirectoryExtension;
import org.neo4j.values.storable.Values;
import org.neo4j.values.virtual.MapValueBuilder;
import org.neo4j.values.virtual.PathValue;

@EphemeralTestDirectoryExtension
@Neo4jWithSocketExtension
public class UnsupportedStructTypesV1V2IT extends AbstractBoltTransportsTest {

    @Inject
    private Neo4jWithSocket server;

    @BeforeEach
    public void setup(TestInfo testInfo) throws IOException {
        server.setConfigure(getSettingsFunction());
        server.init(testInfo);
        address = server.lookupDefaultConnector();
    }

    private void sendRun(Consumer<PackstreamBuf> packer) throws IOException {
        var buf = PackstreamBuf.allocUnpooled()
                .writeStructHeader(new StructHeader(3, BoltV40Wire.MESSAGE_TAG_RUN))
                .writeString("RETURN $x") // statement
                .writeMapHeader(1) // parameters
                .writeString("x");

        packer.accept(buf);

        this.connection.send(buf.writeMapHeader(0) // extra
                .getTarget());
    }

    @ParameterizedTest(name = "{displayName} {arguments}")
    @MethodSource("argumentsProvider")
    public void shouldFailWhenNullKeyIsSent(TransportConnection.Factory connectionFactory) throws Exception {
        this.initConnection(connectionFactory);

        this.sendRun(buf -> buf.writeMapHeader(1).writeNull().writeString("foo"));

        assertThat(this.connection)
                .receivesFailure(
                        Status.Request.Invalid,
                        "Illegal value for field \"params\": Unexpected type: Expected STRING but got NONE");
    }

    @ParameterizedTest(name = "{displayName} {arguments}")
    @MethodSource("argumentsProvider")
    public void shouldFailWhenDuplicateKeyIsSent(TransportConnection.Factory connectionFactory) throws Exception {
        this.initConnection(connectionFactory);

        this.sendRun(buf -> buf.writeMapHeader(2)
                .writeString("foo")
                .writeString("bar")
                .writeString("foo")
                .writeString("changed_my_mind"));

        assertThat(this.connection)
                .receivesFailure(
                        Status.Request.Invalid, "Illegal value for field \"params\": Duplicate map key: \"foo\"");
    }

    @ParameterizedTest(name = "{displayName} {arguments}")
    @MethodSource("argumentsProvider")
    public void shouldFailWhenNodeIsSentWithRun(TransportConnection.Factory connectionFactory) throws Exception {
        this.initConnection(connectionFactory);

        var properties = new MapValueBuilder();
        properties.add("the_answer", longValue(42));
        properties.add("one_does_not_simply", stringValue("break_decoding"));

        this.sendRun(buf -> new LegacyBoltValueWriter(buf)
                .writeNode("42", 42, Values.stringArray("Broken", "Dreams"), properties.build(), false));

        assertThat(this.connection)
                .receivesFailure(
                        Status.Request.Invalid, "Illegal value for field \"params\": Unexpected struct tag: 0x4E");
    }

    @ParameterizedTest(name = "{displayName} {arguments}")
    @MethodSource("argumentsProvider")
    public void shouldFailWhenRelationshipIsSentWithRun(TransportConnection.Factory connectionFactory)
            throws Exception {
        this.initConnection(connectionFactory);

        var properties = new MapValueBuilder();
        properties.add("the_answer", longValue(42));
        properties.add("one_does_not_simply", stringValue("break_decoding"));

        this.sendRun(buf -> new LegacyBoltValueWriter(buf)
                .writeRelationship(
                        "42", 42, "21", 21, "84", 84, stringValue("RUINS_EXPECTATIONS"), properties.build(), false));

        assertThat(this.connection)
                .receivesFailure(
                        Status.Request.Invalid, "Illegal value for field \"params\": Unexpected struct tag: 0x52");
    }

    @ParameterizedTest(name = "{displayName} {arguments}")
    @MethodSource("argumentsProvider")
    public void shouldFailWhenPathIsSentWithRun(TransportConnection.Factory connectionFactory) throws Exception {
        this.initConnection(connectionFactory);

        for (PathValue path : ALL_PATHS) {
            this.sendRun(buf -> path.writeTo(new LegacyBoltValueWriter(buf)));

            assertThat(connection)
                    .receivesFailure(
                            Status.Request.Invalid, "Illegal value for field \"params\": Unexpected struct tag: 0x50");

            this.connection.send(reset());

            assertThat(connection).receivesSuccess();
        }
    }

    @ParameterizedTest(name = "{displayName} {arguments}")
    @MethodSource("argumentsProvider")
    public void shouldTerminateConnectionWhenUnknownMessageIsSent(TransportConnection.Factory connectionFactory)
            throws Exception {
        this.initConnection(connectionFactory);

        this.connection.send(PackstreamBuf.allocUnpooled()
                .writeStructHeader(new StructHeader(1, (short) 0x42))
                .writeListHeader(1)
                .writeInt(42));

        assertThat(connection).isEventuallyTerminated();
    }
}
