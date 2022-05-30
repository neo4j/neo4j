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
import static org.neo4j.bolt.transport.Neo4jWithSocket.withOptionalBoltEncryption;

import io.netty.buffer.ByteBuf;
import java.io.IOException;
import java.util.function.Consumer;
import java.util.stream.Stream;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.neo4j.bolt.testing.client.TransportConnection;
import org.neo4j.bolt.testing.messages.BoltV40Wire;
import org.neo4j.internal.helpers.HostnamePort;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.packstream.io.NativeStruct;
import org.neo4j.packstream.io.NativeStructType;
import org.neo4j.packstream.io.PackstreamBuf;
import org.neo4j.packstream.struct.StructHeader;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.testdirectory.EphemeralTestDirectoryExtension;
import org.neo4j.values.storable.CoordinateReferenceSystem;

@EphemeralTestDirectoryExtension
@Neo4jWithSocketExtension
public class UnsupportedStructTypesV2IT {
    @Inject
    private Neo4jWithSocket server;

    private HostnamePort address;
    private TransportConnection connection;

    @BeforeEach
    public void setup(TestInfo testInfo) throws IOException {
        server.setConfigure(withOptionalBoltEncryption());
        server.init(testInfo);
        address = server.lookupDefaultConnector();
    }

    @AfterEach
    public void cleanup() throws Exception {
        if (connection != null) {
            connection.disconnect();
        }
    }

    public static Stream<TransportConnection.Factory> factoryProvider() {
        return TransportConnection.factories();
    }

    private void initConnection(TransportConnection.Factory connectionFactory) throws Exception {
        connection = connectionFactory.create(this.address);
    }

    @ParameterizedTest(name = "{displayName} {0}")
    @MethodSource("factoryProvider")
    public void shouldFailWhenPoint2DIsSentWithInvalidCrsId(TransportConnection.Factory connectionFactory)
            throws Exception {
        initConnection(connectionFactory);

        testFailureWithUnpackableValue(
                buf -> buf.writeStructHeader(new StructHeader(3, NativeStructType.POINT_2D.getTag()))
                        .writeInt(5) // CRS
                        .writeFloat(3.15) // X
                        .writeFloat(4.012), // Y
                "Illegal value for field \"params\": Illegal value for field \"crs\": Illegal coordinate reference system: \"5\"");
    }

    @ParameterizedTest(name = "{displayName} {0}")
    @MethodSource("factoryProvider")
    public void shouldFailWhenPoint3DIsSentWithInvalidCrsId(TransportConnection.Factory connectionFactory)
            throws Exception {
        initConnection(connectionFactory);

        testFailureWithUnpackableValue(
                buf -> buf.writeStructHeader(new StructHeader(4, NativeStructType.POINT_3D.getTag()))
                        .writeInt(1200) // CRS
                        .writeFloat(3.15)
                        .writeFloat(4.012)
                        .writeFloat(5.905),
                "Illegal value for field \"params\": Illegal value for field \"crs\": Illegal coordinate reference system: \"1200\"");
    }

    @ParameterizedTest(name = "{displayName} {0}")
    @MethodSource("factoryProvider")
    public void shouldFailWhenPoint2DDimensionsDoNotMatch(TransportConnection.Factory connectionFactory)
            throws Exception {
        initConnection(connectionFactory);

        testFailureWithUnpackableValue(
                buf -> NativeStruct.writePoint2d(buf, CoordinateReferenceSystem.CARTESIAN_3D, 3.15, 4.012),
                "Illegal value for field \"params\": Illegal value for field \"coords\": Illegal CRS/coords combination (crs=cartesian-3d, x=3.15, y=4.012)");
    }

    @ParameterizedTest(name = "{displayName} {0}")
    @MethodSource("factoryProvider")
    public void shouldFailWhenPoint3DDimensionsDoNotMatch(TransportConnection.Factory connectionFactory)
            throws Exception {
        initConnection(connectionFactory);

        testFailureWithUnpackableValue(
                buf -> NativeStruct.writePoint3d(buf, CoordinateReferenceSystem.CARTESIAN, 3.15, 4.012, 5.905),
                "Illegal value for field \"params\": Illegal value for field \"coords\": Illegal CRS/coords combination (crs=cartesian, x=3.15, y=4.012, z=5.905)");
    }

    @ParameterizedTest(name = "{displayName} {0}")
    @MethodSource("factoryProvider")
    public void shouldFailWhenZonedDateTimeZoneIdIsNotKnown(TransportConnection.Factory connectionFactory)
            throws Exception {
        initConnection(connectionFactory);

        testFailureWithUnpackableValue(
                buf -> buf.writeStructHeader(new StructHeader(3, NativeStructType.DATE_TIME_ZONE_ID.getTag()))
                        .writeInt(0)
                        .writeInt(0)
                        .writeString("Europe/Marmaris"),
                "Illegal value for field \"params\": Illegal value for field \"tz_id\": Illegal zone identifier: \"Europe/Marmaris\"");
    }

    private void testFailureWithUnpackableValue(Consumer<PackstreamBuf> packer, String expectedMessage)
            throws Exception {
        connection.connect().sendDefaultProtocolVersion().send(hello());

        assertThat(connection).negotiatesDefaultVersion();

        assertThat(connection).receivesSuccess();

        connection.send(createRunWith(packer));

        assertThat(connection).receivesFailure(Status.Request.Invalid, expectedMessage);
    }

    private static ByteBuf createRunWith(Consumer<PackstreamBuf> packer) throws IOException {
        var buf = PackstreamBuf.allocUnpooled()
                .writeStructHeader(new StructHeader(3, BoltV40Wire.MESSAGE_TAG_RUN))
                .writeString("RETURN $x") // statement
                .writeMapHeader(1) // parameters
                .writeString("x");

        packer.accept(buf);

        return buf.writeMapHeader(0) // extra
                .getTarget();
    }
}
