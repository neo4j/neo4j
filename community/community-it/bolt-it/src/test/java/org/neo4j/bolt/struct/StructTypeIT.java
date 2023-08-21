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

import static org.neo4j.bolt.testing.assertions.BoltConnectionAssertions.assertThat;
import static org.neo4j.values.storable.CoordinateReferenceSystem.CARTESIAN;
import static org.neo4j.values.storable.CoordinateReferenceSystem.WGS_84;
import static org.neo4j.values.storable.DateTimeValue.datetime;
import static org.neo4j.values.storable.DateValue.date;
import static org.neo4j.values.storable.DurationValue.duration;
import static org.neo4j.values.storable.LocalDateTimeValue.localDateTime;
import static org.neo4j.values.storable.LocalTimeValue.localTime;
import static org.neo4j.values.storable.TimeValue.time;
import static org.neo4j.values.storable.Values.longValue;
import static org.neo4j.values.storable.Values.pointValue;
import static org.neo4j.values.virtual.VirtualValues.map;

import java.time.ZoneOffset;
import org.neo4j.bolt.test.annotation.BoltTestExtension;
import org.neo4j.bolt.test.annotation.connection.initializer.Authenticated;
import org.neo4j.bolt.test.annotation.test.ProtocolTest;
import org.neo4j.bolt.testing.client.TransportConnection;
import org.neo4j.bolt.testing.messages.BoltWire;
import org.neo4j.bolt.transport.Neo4jWithSocketExtension;
import org.neo4j.test.extension.testdirectory.EphemeralTestDirectoryExtension;
import org.neo4j.values.AnyValue;
import org.neo4j.values.virtual.ListValueBuilder;
import org.neo4j.values.virtual.MapValueBuilder;

/**
 * Evaluates whether Bolt is capable of sending and receiving Bolt struct types.
 */
@EphemeralTestDirectoryExtension
@Neo4jWithSocketExtension
@BoltTestExtension
public class StructTypeIT {

    private static <T extends AnyValue> void testSendingOfBoltV2Value(
            BoltWire wire, TransportConnection connection, T value) throws Exception {
        connection
                .send(wire.run(
                        "CREATE (n:Node {value: $value}) RETURN 42",
                        map(new String[] {"value"}, new AnyValue[] {value})))
                .send(wire.pull());

        assertThat(connection).receivesSuccess().receivesRecord(longValue(42)).receivesSuccess();
    }

    private static <T extends AnyValue> void testReceivingOfBoltV2Value(
            BoltWire wire, TransportConnection connection, String query, T expectedValue) throws Exception {
        connection.send(wire.run(query)).send(wire.pull());

        assertThat(connection).receivesSuccess().receivesRecord(expectedValue).receivesSuccess();
    }

    private static <T extends AnyValue> void testSendingAndReceivingOfBoltV2Value(
            BoltWire wire, TransportConnection connection, T value) throws Exception {
        connection
                .send(wire.run("RETURN $value", map(new String[] {"value"}, new AnyValue[] {value})))
                .send(wire.pull());

        assertThat(connection).receivesSuccess().receivesRecord(value).receivesSuccess();
    }

    @ProtocolTest
    void shouldSendPoint2D(BoltWire wire, @Authenticated TransportConnection connection) throws Exception {
        testSendingOfBoltV2Value(wire, connection, pointValue(WGS_84, 39.111748, -76.775635));
    }

    @ProtocolTest
    void shouldReceivePoint2D(BoltWire wire, @Authenticated TransportConnection connection) throws Exception {
        testReceivingOfBoltV2Value(
                wire, connection, "RETURN point({x: 40.7624, y: 73.9738})", pointValue(CARTESIAN, 40.7624, 73.9738));
    }

    @ProtocolTest
    void shouldSendAndReceivePoint2D(BoltWire wire, @Authenticated TransportConnection connection) throws Exception {
        testSendingAndReceivingOfBoltV2Value(wire, connection, pointValue(WGS_84, 38.8719, 77.0563));
    }

    @ProtocolTest
    void shouldSendDuration(BoltWire wire, @Authenticated TransportConnection connection) throws Exception {
        testSendingOfBoltV2Value(wire, connection, duration(5, 3, 34, 0));
    }

    @ProtocolTest
    void shouldReceiveDuration(BoltWire wire, @Authenticated TransportConnection connection) throws Exception {
        testReceivingOfBoltV2Value(
                wire,
                connection,
                "RETURN duration({months: 3, days: 100, seconds: 999, nanoseconds: 42})",
                duration(3, 100, 999, 42));
    }

    @ProtocolTest
    void shouldSendAndReceiveDuration(BoltWire wire, @Authenticated TransportConnection connection) throws Exception {
        testSendingAndReceivingOfBoltV2Value(wire, connection, duration(17, 9, 2, 1_000_000));
    }

    @ProtocolTest
    void shouldSendDate(BoltWire wire, @Authenticated TransportConnection connection) throws Exception {
        testSendingOfBoltV2Value(wire, connection, date(1991, 8, 24));
    }

    @ProtocolTest
    void shouldReceiveDate(BoltWire wire, @Authenticated TransportConnection connection) throws Exception {
        testReceivingOfBoltV2Value(wire, connection, "RETURN date('2015-02-18')", date(2015, 2, 18));
    }

    @ProtocolTest
    void shouldSendAndReceiveDate(BoltWire wire, @Authenticated TransportConnection connection) throws Exception {
        testSendingAndReceivingOfBoltV2Value(wire, connection, date(2005, 5, 22));
    }

    @ProtocolTest
    void shouldSendLocalTime(BoltWire wire, @Authenticated TransportConnection connection) throws Exception {
        testSendingOfBoltV2Value(wire, connection, localTime(2, 35, 10, 1));
    }

    @ProtocolTest
    void shouldReceiveLocalTime(BoltWire wire, @Authenticated TransportConnection connection) throws Exception {
        testReceivingOfBoltV2Value(wire, connection, "RETURN localtime('11:04:35')", localTime(11, 04, 35, 0));
    }

    @ProtocolTest
    void shouldSendAndReceiveLocalTime(BoltWire wire, @Authenticated TransportConnection connection) throws Exception {
        testSendingAndReceivingOfBoltV2Value(wire, connection, localTime(22, 10, 10, 99));
    }

    @ProtocolTest
    void shouldSendTime(BoltWire wire, @Authenticated TransportConnection connection) throws Exception {
        testSendingOfBoltV2Value(wire, connection, time(424242, ZoneOffset.of("+08:30")));
    }

    @ProtocolTest
    void shouldReceiveTime(BoltWire wire, @Authenticated TransportConnection connection) throws Exception {
        testReceivingOfBoltV2Value(
                wire, connection, "RETURN time('14:30+0100')", time(14, 30, 0, 0, ZoneOffset.ofHours(1)));
    }

    @ProtocolTest
    void shouldSendAndReceiveTime(BoltWire wire, @Authenticated TransportConnection connection) throws Exception {
        testSendingAndReceivingOfBoltV2Value(wire, connection, time(19, 22, 44, 100, ZoneOffset.ofHours(-5)));
    }

    @ProtocolTest
    void shouldSendLocalDateTime(BoltWire wire, @Authenticated TransportConnection connection) throws Exception {
        testSendingOfBoltV2Value(wire, connection, localDateTime(2002, 5, 22, 15, 15, 25, 0));
    }

    @ProtocolTest
    void shouldReceiveLocalDateTime(BoltWire wire, @Authenticated TransportConnection connection) throws Exception {
        testReceivingOfBoltV2Value(
                wire,
                connection,
                "RETURN localdatetime('20150202T19:32:24')",
                localDateTime(2015, 2, 2, 19, 32, 24, 0));
    }

    @ProtocolTest
    void shouldSendAndReceiveLocalDateTime(BoltWire wire, @Authenticated TransportConnection connection)
            throws Exception {
        testSendingAndReceivingOfBoltV2Value(wire, connection, localDateTime(1995, 12, 12, 10, 30, 0, 0));
    }

    @ProtocolTest
    void shouldSendDateTimeWithTimeZoneName(BoltWire wire, @Authenticated TransportConnection connection)
            throws Exception {
        testSendingOfBoltV2Value(wire, connection, datetime(1956, 9, 14, 11, 20, 25, 0, "Europe/Stockholm"));
    }

    @ProtocolTest
    void shouldReceiveDateTimeWithTimeZoneName(BoltWire wire, @Authenticated TransportConnection connection)
            throws Exception {
        testReceivingOfBoltV2Value(
                wire,
                connection,
                "RETURN datetime({year:1984, month:10, day:11, hour:21, minute:30, timezone:'Europe/London'})",
                datetime(1984, 10, 11, 21, 30, 0, 0, "Europe/London"));
    }

    @ProtocolTest
    void shouldSendAndReceiveDateTimeWithTimeZoneName(BoltWire wire, @Authenticated TransportConnection connection)
            throws Exception {
        testSendingAndReceivingOfBoltV2Value(wire, connection, datetime(1984, 10, 11, 21, 30, 0, 0, "Europe/London"));
    }

    @ProtocolTest
    void shouldSendDateTimeWithTimeZoneOffset(BoltWire wire, @Authenticated TransportConnection connection)
            throws Exception {
        testSendingOfBoltV2Value(wire, connection, datetime(424242, 0, ZoneOffset.ofHoursMinutes(-7, -15)));
    }

    @ProtocolTest
    void shouldReceiveDateTimeWithTimeZoneOffset(BoltWire wire, @Authenticated TransportConnection connection)
            throws Exception {
        testReceivingOfBoltV2Value(
                wire,
                connection,
                "RETURN datetime({year:2022, month:3, day:2, hour:19, minute:10, timezone:'+02:30'})",
                datetime(2022, 3, 2, 19, 10, 0, 0, ZoneOffset.ofHoursMinutes(2, 30)));
    }

    @ProtocolTest
    void shouldSendAndReceiveDateTimeWithTimeZoneOffset(BoltWire wire, @Authenticated TransportConnection connection)
            throws Exception {
        testSendingAndReceivingOfBoltV2Value(
                wire, connection, datetime(1899, 1, 1, 12, 12, 32, 0, ZoneOffset.ofHoursMinutes(-4, -15)));
    }

    @ProtocolTest
    public void shouldSendAndReceiveMapContainingStruct(BoltWire wire, @Authenticated TransportConnection connection)
            throws Exception {
        var mapValueBuilder = new MapValueBuilder();
        mapValueBuilder.add("foo", datetime(1899, 1, 1, 12, 12, 32, 0, ZoneOffset.ofHoursMinutes(-4, -15)));

        testSendingAndReceivingOfBoltV2Value(wire, connection, mapValueBuilder.build());
    }

    @ProtocolTest
    public void shouldSendAndReceiveListContainingStruct(BoltWire wire, @Authenticated TransportConnection connection)
            throws Exception {
        var listValueBuilder = ListValueBuilder.newListBuilder();
        listValueBuilder.add(datetime(1899, 1, 1, 12, 12, 32, 0, ZoneOffset.ofHoursMinutes(-4, -15)));

        testSendingAndReceivingOfBoltV2Value(wire, connection, listValueBuilder.build());
    }
}
