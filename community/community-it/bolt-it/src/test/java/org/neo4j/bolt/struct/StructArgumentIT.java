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
package org.neo4j.bolt.struct;

import java.io.IOException;
import org.neo4j.bolt.protocol.common.connector.connection.Feature;
import org.neo4j.bolt.protocol.io.StructType;
import org.neo4j.bolt.test.annotation.BoltTestExtension;
import org.neo4j.bolt.test.annotation.connection.initializer.Authenticated;
import org.neo4j.bolt.test.annotation.test.ProtocolTest;
import org.neo4j.bolt.test.annotation.wire.initializer.EnableFeature;
import org.neo4j.bolt.testing.client.TransportConnection;
import org.neo4j.bolt.transport.Neo4jWithSocketExtension;
import org.neo4j.packstream.struct.StructHeader;
import org.neo4j.test.extension.testdirectory.EphemeralTestDirectoryExtension;
import org.neo4j.values.storable.CoordinateReferenceSystem;

@EphemeralTestDirectoryExtension
@Neo4jWithSocketExtension
@BoltTestExtension
public class StructArgumentIT extends AbstractStructArgumentIT {

    @ProtocolTest
    void shouldFailWhenPoint2DIsSentWithInvalidCrsId(@Authenticated TransportConnection connection) throws IOException {
        testFailureWithUnpackableValue(
                connection,
                buf -> buf.writeStructHeader(new StructHeader(3, StructType.POINT_2D.getTag()))
                        .writeInt(5) // CRS
                        .writeFloat(3.15) // X
                        .writeFloat(4.012), // Y
                "Illegal value for field \"params\": Illegal value for field \"crs\": Illegal coordinate reference system: \"5\"");
    }

    @ProtocolTest
    void shouldFailWhenPoint3DIsSentWithInvalidCrsId(@Authenticated TransportConnection connection) throws IOException {
        testFailureWithUnpackableValue(
                connection,
                buf -> buf.writeStructHeader(new StructHeader(4, StructType.POINT_3D.getTag()))
                        .writeInt(1200) // CRS
                        .writeFloat(3.15)
                        .writeFloat(4.012)
                        .writeFloat(5.905),
                "Illegal value for field \"params\": Illegal value for field \"crs\": Illegal coordinate reference system: \"1200\"");
    }

    @ProtocolTest
    void shouldFailWhenPoint2DDimensionsDoNotMatch(@Authenticated TransportConnection connection) throws IOException {
        testFailureWithUnpackableValue(
                connection,
                buf -> buf.writeStructHeader(new StructHeader(3, StructType.POINT_2D.getTag()))
                        .writeInt(CoordinateReferenceSystem.CARTESIAN_3D.getCode())
                        .writeFloat(3.15)
                        .writeFloat(4.012),
                "Illegal value for field \"params\": Illegal value for field \"coords\": Illegal CRS/coords combination (crs=cartesian-3d, x=3.15, y=4.012)");
    }

    @ProtocolTest
    void shouldFailWhenPoint3DDimensionsDoNotMatch(@Authenticated TransportConnection connection) throws IOException {
        testFailureWithUnpackableValue(
                connection,
                buf -> buf.writeStructHeader(new StructHeader(4, StructType.POINT_3D.getTag()))
                        .writeInt(CoordinateReferenceSystem.CARTESIAN.getCode())
                        .writeFloat(3.15)
                        .writeFloat(4.012)
                        .writeFloat(5.905),
                "Illegal value for field \"params\": Illegal value for field \"coords\": Illegal CRS/coords combination (crs=cartesian, x=3.15, y=4.012, z=5.905)");
    }

    @ProtocolTest
    @EnableFeature(Feature.UTC_DATETIME)
    void shouldFailWhenZonedDateTimeZoneIdIsNotKnown(@Authenticated TransportConnection connection) throws IOException {
        testFailureWithUnpackableValue(
                connection,
                buf -> buf.writeStructHeader(new StructHeader(3, StructType.DATE_TIME_ZONE_ID.getTag()))
                        .writeInt(0)
                        .writeInt(0)
                        .writeString("Europe/Marmaris"),
                "Illegal value for field \"params\": Illegal value for field \"tz_id\": Illegal zone identifier: \"Europe/Marmaris\"");
    }
}
