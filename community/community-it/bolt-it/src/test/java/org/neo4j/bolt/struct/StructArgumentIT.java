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

import org.neo4j.bolt.protocol.common.connector.connection.Feature;
import org.neo4j.bolt.protocol.io.StructType;
import org.neo4j.bolt.test.annotation.BoltTestExtension;
import org.neo4j.bolt.test.annotation.connection.initializer.Authenticated;
import org.neo4j.bolt.test.annotation.test.ProtocolTest;
import org.neo4j.bolt.test.annotation.wire.initializer.EnableFeature;
import org.neo4j.bolt.testing.assertions.DiagnosticRecordAssertions;
import org.neo4j.bolt.testing.assertions.FailureCauseAssertions;
import org.neo4j.bolt.testing.assertions.GqlMessageParameters;
import org.neo4j.bolt.testing.client.BoltTestConnection;
import org.neo4j.bolt.transport.Neo4jWithSocketExtension;
import org.neo4j.gqlstatus.ErrorClassification;
import org.neo4j.gqlstatus.GqlStatusInfoCodes;
import org.neo4j.packstream.struct.StructHeader;
import org.neo4j.test.extension.testdirectory.EphemeralTestDirectoryExtension;
import org.neo4j.values.storable.CoordinateReferenceSystem;

@EphemeralTestDirectoryExtension
@Neo4jWithSocketExtension
@BoltTestExtension
public class StructArgumentIT extends AbstractStructArgumentIT {

    @ProtocolTest
    void shouldFailWhenPoint2DIsSentWithInvalidCrsId(@Authenticated BoltTestConnection connection) {
        testFailureWithUnpackableValue(
                connection,
                buf -> buf.writeStructHeader(new StructHeader(3, StructType.POINT_2D.getTag()))
                        .writeInt(5) // CRS
                        .writeFloat64(3.15) // X
                        .writeFloat64(4.012), // Y
                "Illegal value for field \"params\": Illegal value for field \"crs\": Illegal coordinate reference system: \"5\"",
                FailureCauseAssertions.create()
                        .hasStatus(GqlStatusInfoCodes.STATUS_08N06)
                        .hasDescription("error: connection exception - protocol error. General network protocol error.")
                        .hasDiagnosticRecord(
                                DiagnosticRecordAssertions.create().hasClassification(ErrorClassification.CLIENT_ERROR))
                        .hasCause(FailureCauseAssertions.create()
                                .hasStatus(GqlStatusInfoCodes.STATUS_22000)
                                .hasDescription("error: data exception")
                                .hasDiagnosticRecord(DiagnosticRecordAssertions.create()
                                        .hasClassification(ErrorClassification.CLIENT_ERROR))
                                .hasCause(FailureCauseAssertions.create()
                                        .hasStatus(
                                                GqlStatusInfoCodes.STATUS_22N21,
                                                GqlMessageParameters.create().withString("code=5"))
                                        .hasDescription(
                                                "error: data exception - unsupported coordinate reference system. Unsupported coordinate reference system (CRS): code=5.")
                                        .hasDiagnosticRecord(DiagnosticRecordAssertions.create()
                                                .hasClassification(ErrorClassification.CLIENT_ERROR)))));
    }

    @ProtocolTest
    void shouldFailWhenPoint3DIsSentWithInvalidCrsId(@Authenticated BoltTestConnection connection) {
        testFailureWithUnpackableValue(
                connection,
                buf -> buf.writeStructHeader(new StructHeader(4, StructType.POINT_3D.getTag()))
                        .writeInt(1200) // CRS
                        .writeFloat64(3.15)
                        .writeFloat64(4.012)
                        .writeFloat64(5.905),
                "Illegal value for field \"params\": Illegal value for field \"crs\": Illegal coordinate reference system: \"1200\"",
                FailureCauseAssertions.create()
                        .hasStatus(GqlStatusInfoCodes.STATUS_08N06)
                        .hasDescription("error: connection exception - protocol error. General network protocol error.")
                        .hasDiagnosticRecord(
                                DiagnosticRecordAssertions.create().hasClassification(ErrorClassification.CLIENT_ERROR))
                        .hasCause(FailureCauseAssertions.create()
                                .hasStatus(GqlStatusInfoCodes.STATUS_22000)
                                .hasDescription("error: data exception")
                                .hasDiagnosticRecord(DiagnosticRecordAssertions.create()
                                        .hasClassification(ErrorClassification.CLIENT_ERROR))
                                .hasCause(FailureCauseAssertions.create()
                                        .hasStatus(
                                                GqlStatusInfoCodes.STATUS_22N21,
                                                GqlMessageParameters.create().withString("code=1200"))
                                        .hasDescription(
                                                "error: data exception - unsupported coordinate reference system. Unsupported coordinate reference system (CRS): code=1200.")
                                        .hasDiagnosticRecord(DiagnosticRecordAssertions.create()
                                                .hasClassification(ErrorClassification.CLIENT_ERROR)))));
    }

    @ProtocolTest
    void shouldFailWhenPoint2DDimensionsDoNotMatch(@Authenticated BoltTestConnection connection) {
        testFailureWithUnpackableValue(
                connection,
                buf -> buf.writeStructHeader(new StructHeader(3, StructType.POINT_2D.getTag()))
                        .writeInt(CoordinateReferenceSystem.CARTESIAN_3D.getCode())
                        .writeFloat64(3.15)
                        .writeFloat64(4.012),
                "Illegal value for field \"params\": Illegal value for field \"coords\": Illegal CRS/coords combination (crs=cartesian-3d, x=3.15, y=4.012)",
                FailureCauseAssertions.create()
                        .hasStatus(GqlStatusInfoCodes.STATUS_08N06)
                        .hasDescription("error: connection exception - protocol error. General network protocol error.")
                        .hasDiagnosticRecord(
                                DiagnosticRecordAssertions.create().hasClassification(ErrorClassification.CLIENT_ERROR))
                        .hasCause(FailureCauseAssertions.create()
                                .hasStatus(
                                        GqlStatusInfoCodes.STATUS_22N24,
                                        GqlMessageParameters.create()
                                                .withString("point")
                                                .withList(3.15f, 4.012f))
                                .hasDescription(
                                        "error: data exception - invalid coordinate arguments. Cannot construct a point from [3.15, 4.012].")
                                .hasDiagnosticRecord(DiagnosticRecordAssertions.create()
                                        .hasClassification(ErrorClassification.CLIENT_ERROR))
                                .hasCause(FailureCauseAssertions.create()
                                        .hasStatus(
                                                GqlStatusInfoCodes.STATUS_22N20,
                                                GqlMessageParameters.create()
                                                        .withInt(3)
                                                        .withInt(2)
                                                        .withInt(2))
                                        .hasDescription(
                                                "error: data exception - invalid spatial value dimensions. Cannot create POINT with 3D coordinate reference system (CRS) and 2 coordinates. Use the equivalent 2D coordinate reference system instead.")
                                        .hasDiagnosticRecord(DiagnosticRecordAssertions.create()
                                                .hasClassification(ErrorClassification.CLIENT_ERROR)))));
    }

    @ProtocolTest
    void shouldFailWhenPoint3DDimensionsDoNotMatch(@Authenticated BoltTestConnection connection) {
        testFailureWithUnpackableValue(
                connection,
                buf -> buf.writeStructHeader(new StructHeader(4, StructType.POINT_3D.getTag()))
                        .writeInt(CoordinateReferenceSystem.CARTESIAN.getCode())
                        .writeFloat64(3.15)
                        .writeFloat64(4.012)
                        .writeFloat64(5.905),
                "Illegal value for field \"params\": Illegal value for field \"coords\": Illegal CRS/coords combination (crs=cartesian, x=3.15, y=4.012, z=5.905)",
                FailureCauseAssertions.create()
                        .hasStatus(GqlStatusInfoCodes.STATUS_08N06)
                        .hasDescription("error: connection exception - protocol error. General network protocol error.")
                        .hasDiagnosticRecord(
                                DiagnosticRecordAssertions.create().hasClassification(ErrorClassification.CLIENT_ERROR))
                        .hasCause(FailureCauseAssertions.create()
                                .hasStatus(
                                        GqlStatusInfoCodes.STATUS_22N24,
                                        GqlMessageParameters.create()
                                                .withString("point")
                                                .withList(3.15, 4.012, 5.905))
                                .hasDescription(
                                        "error: data exception - invalid coordinate arguments. Cannot construct a point from [3.15, 4.012, 5.905].")
                                .hasDiagnosticRecord(DiagnosticRecordAssertions.create()
                                        .hasClassification(ErrorClassification.CLIENT_ERROR))
                                .hasCause(FailureCauseAssertions.create()
                                        .hasStatus(
                                                GqlStatusInfoCodes.STATUS_22N20,
                                                GqlMessageParameters.create()
                                                        .withInt(2)
                                                        .withInt(3)
                                                        .withInt(3))
                                        .hasDescription(
                                                "error: data exception - invalid spatial value dimensions. Cannot create POINT with 2D coordinate reference system (CRS) and 3 coordinates. Use the equivalent 3D coordinate reference system instead.")
                                        .hasDiagnosticRecord(DiagnosticRecordAssertions.create()
                                                .hasClassification(ErrorClassification.CLIENT_ERROR)))));
    }

    @ProtocolTest
    @EnableFeature(Feature.UTC_DATETIME)
    void shouldFailWhenZonedDateTimeZoneIdIsNotKnown(@Authenticated BoltTestConnection connection) {
        testFailureWithUnpackableValue(
                connection,
                buf -> buf.writeStructHeader(new StructHeader(3, StructType.DATE_TIME_ZONE_ID.getTag()))
                        .writeInt(0)
                        .writeInt(0)
                        .writeString("Europe/Marmaris"),
                "Illegal value for field \"params\": Illegal value for field \"tz_id\": Illegal zone identifier: \"Europe/Marmaris\"",
                FailureCauseAssertions.create()
                        .hasStatus(GqlStatusInfoCodes.STATUS_08N06)
                        .hasDescription("error: connection exception - protocol error. General network protocol error.")
                        .hasDiagnosticRecord(
                                DiagnosticRecordAssertions.create().hasClassification(ErrorClassification.CLIENT_ERROR))
                        .hasCause(FailureCauseAssertions.create()
                                .hasStatus(
                                        GqlStatusInfoCodes.STATUS_22NB5,
                                        GqlMessageParameters.create().withString("Europe/Marmaris"))
                                .hasDescription(
                                        "error: data exception - unsupported time zone identifier. Unknown time zone identifier 'Europe/Marmaris'.")
                                .hasDiagnosticRecord(DiagnosticRecordAssertions.create()
                                        .hasClassification(ErrorClassification.CLIENT_ERROR))));
    }
}
