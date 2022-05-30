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
import static org.neo4j.bolt.testing.messages.BoltDefaultWire.pull;
import static org.neo4j.bolt.testing.messages.BoltDefaultWire.run;
import static org.neo4j.bolt.transport.Neo4jWithSocket.withOptionalBoltEncryption;
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

import java.io.IOException;
import java.time.ZoneOffset;
import java.util.stream.Stream;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.neo4j.bolt.testing.client.TransportConnection;
import org.neo4j.internal.helpers.HostnamePort;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.testdirectory.EphemeralTestDirectoryExtension;
import org.neo4j.values.AnyValue;

@EphemeralTestDirectoryExtension
@Neo4jWithSocketExtension
public class SupportedStructTypesV2IT {
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
    public void tearDown() throws IOException {
        if (connection != null) {
            connection.disconnect();
        }
    }

    public static Stream<TransportConnection.Factory> factoryProvider() {
        return TransportConnection.factories();
    }

    private void initConnection(TransportConnection.Factory connectionFactory) {
        connection = connectionFactory.create(address);
    }

    @ParameterizedTest(name = "{displayName} {0}")
    @MethodSource("factoryProvider")
    public void shouldSendPoint2D(TransportConnection.Factory connectionFactory) throws Exception {
        initConnection(connectionFactory);

        testSendingOfBoltV2Value(pointValue(WGS_84, 39.111748, -76.775635));
    }

    @ParameterizedTest(name = "{displayName} {0}")
    @MethodSource("factoryProvider")
    public void shouldReceivePoint2D(TransportConnection.Factory connectionFactory) throws Exception {
        initConnection(connectionFactory);

        testReceivingOfBoltV2Value("RETURN point({x: 40.7624, y: 73.9738})", pointValue(CARTESIAN, 40.7624, 73.9738));
    }

    @ParameterizedTest(name = "{displayName} {0}")
    @MethodSource("factoryProvider")
    public void shouldSendAndReceivePoint2D(TransportConnection.Factory connectionFactory) throws Exception {
        initConnection(connectionFactory);

        testSendingAndReceivingOfBoltV2Value(pointValue(WGS_84, 38.8719, 77.0563));
    }

    @ParameterizedTest(name = "{displayName} {0}")
    @MethodSource("factoryProvider")
    public void shouldSendDuration(TransportConnection.Factory connectionFactory) throws Exception {
        initConnection(connectionFactory);

        testSendingOfBoltV2Value(duration(5, 3, 34, 0));
    }

    @ParameterizedTest(name = "{displayName} {0}")
    @MethodSource("factoryProvider")
    public void shouldReceiveDuration(TransportConnection.Factory connectionFactory) throws Exception {
        initConnection(connectionFactory);

        testReceivingOfBoltV2Value(
                "RETURN duration({months: 3, days: 100, seconds: 999, nanoseconds: 42})", duration(3, 100, 999, 42));
    }

    @ParameterizedTest(name = "{displayName} {0}")
    @MethodSource("factoryProvider")
    public void shouldSendAndReceiveDuration(TransportConnection.Factory connectionFactory) throws Exception {
        initConnection(connectionFactory);

        testSendingAndReceivingOfBoltV2Value(duration(17, 9, 2, 1_000_000));
    }

    @ParameterizedTest(name = "{displayName} {0}")
    @MethodSource("factoryProvider")
    public void shouldSendDate(TransportConnection.Factory connectionFactory) throws Exception {
        initConnection(connectionFactory);

        testSendingOfBoltV2Value(date(1991, 8, 24));
    }

    @ParameterizedTest(name = "{displayName} {0}")
    @MethodSource("factoryProvider")
    public void shouldReceiveDate(TransportConnection.Factory connectionFactory) throws Exception {
        initConnection(connectionFactory);

        testReceivingOfBoltV2Value("RETURN date('2015-02-18')", date(2015, 2, 18));
    }

    @ParameterizedTest(name = "{displayName} {0}")
    @MethodSource("factoryProvider")
    public void shouldSendAndReceiveDate(TransportConnection.Factory connectionFactory) throws Exception {
        initConnection(connectionFactory);

        testSendingAndReceivingOfBoltV2Value(date(2005, 5, 22));
    }

    @ParameterizedTest(name = "{displayName} {0}")
    @MethodSource("factoryProvider")
    public void shouldSendLocalTime(TransportConnection.Factory connectionFactory) throws Exception {
        initConnection(connectionFactory);

        testSendingOfBoltV2Value(localTime(2, 35, 10, 1));
    }

    @ParameterizedTest(name = "{displayName} {0}")
    @MethodSource("factoryProvider")
    public void shouldReceiveLocalTime(TransportConnection.Factory connectionFactory) throws Exception {
        initConnection(connectionFactory);

        testReceivingOfBoltV2Value("RETURN localtime('11:04:35')", localTime(11, 04, 35, 0));
    }

    @ParameterizedTest(name = "{displayName} {0}")
    @MethodSource("factoryProvider")
    public void shouldSendAndReceiveLocalTime(TransportConnection.Factory connectionFactory) throws Exception {
        initConnection(connectionFactory);

        testSendingAndReceivingOfBoltV2Value(localTime(22, 10, 10, 99));
    }

    @ParameterizedTest(name = "{displayName} {0}")
    @MethodSource("factoryProvider")
    public void shouldSendTime(TransportConnection.Factory connectionFactory) throws Exception {
        initConnection(connectionFactory);

        testSendingOfBoltV2Value(time(424242, ZoneOffset.of("+08:30")));
    }

    @ParameterizedTest(name = "{displayName} {0}")
    @MethodSource("factoryProvider")
    public void shouldReceiveTime(TransportConnection.Factory connectionFactory) throws Exception {
        initConnection(connectionFactory);

        testReceivingOfBoltV2Value("RETURN time('14:30+0100')", time(14, 30, 0, 0, ZoneOffset.ofHours(1)));
    }

    @ParameterizedTest(name = "{displayName} {0}")
    @MethodSource("factoryProvider")
    public void shouldSendAndReceiveTime(TransportConnection.Factory connectionFactory) throws Exception {
        initConnection(connectionFactory);

        testSendingAndReceivingOfBoltV2Value(time(19, 22, 44, 100, ZoneOffset.ofHours(-5)));
    }

    @ParameterizedTest(name = "{displayName} {0}")
    @MethodSource("factoryProvider")
    public void shouldSendLocalDateTime(TransportConnection.Factory connectionFactory) throws Exception {
        initConnection(connectionFactory);

        testSendingOfBoltV2Value(localDateTime(2002, 5, 22, 15, 15, 25, 0));
    }

    @ParameterizedTest(name = "{displayName} {0}")
    @MethodSource("factoryProvider")
    public void shouldReceiveLocalDateTime(TransportConnection.Factory connectionFactory) throws Exception {
        initConnection(connectionFactory);

        testReceivingOfBoltV2Value(
                "RETURN localdatetime('20150202T19:32:24')", localDateTime(2015, 2, 2, 19, 32, 24, 0));
    }

    @ParameterizedTest(name = "{displayName} {0}")
    @MethodSource("factoryProvider")
    public void shouldSendAndReceiveLocalDateTime(TransportConnection.Factory connectionFactory) throws Exception {
        initConnection(connectionFactory);

        testSendingAndReceivingOfBoltV2Value(localDateTime(1995, 12, 12, 10, 30, 0, 0));
    }

    @ParameterizedTest(name = "{displayName} {0}")
    @MethodSource("factoryProvider")
    public void shouldSendDateTimeWithTimeZoneName(TransportConnection.Factory connectionFactory) throws Exception {
        initConnection(connectionFactory);

        testSendingOfBoltV2Value(datetime(1956, 9, 14, 11, 20, 25, 0, "Europe/Stockholm"));
    }

    @ParameterizedTest(name = "{displayName} {0}")
    @MethodSource("factoryProvider")
    public void shouldReceiveDateTimeWithTimeZoneName(TransportConnection.Factory connectionFactory) throws Exception {
        initConnection(connectionFactory);

        testReceivingOfBoltV2Value(
                "RETURN datetime({year:1984, month:10, day:11, hour:21, minute:30, timezone:'Europe/London'})",
                datetime(1984, 10, 11, 21, 30, 0, 0, "Europe/London"));
    }

    @ParameterizedTest(name = "{displayName} {0}")
    @MethodSource("factoryProvider")
    public void shouldSendAndReceiveDateTimeWithTimeZoneName(TransportConnection.Factory connectionFactory)
            throws Exception {
        initConnection(connectionFactory);

        testSendingAndReceivingOfBoltV2Value(datetime(1984, 10, 11, 21, 30, 0, 0, "Europe/London"));
    }

    @ParameterizedTest(name = "{displayName} {0}")
    @MethodSource("factoryProvider")
    public void shouldSendDateTimeWithTimeZoneOffset(TransportConnection.Factory connectionFactory) throws Exception {
        initConnection(connectionFactory);

        testSendingOfBoltV2Value(datetime(424242, 0, ZoneOffset.ofHoursMinutes(-7, -15)));
    }

    @ParameterizedTest(name = "{displayName} {0}")
    @MethodSource("factoryProvider")
    public void shouldReceiveDateTimeWithTimeZoneOffset(TransportConnection.Factory connectionFactory)
            throws Exception {
        initConnection(connectionFactory);

        testReceivingOfBoltV2Value(
                "RETURN datetime({year:2022, month:3, day:2, hour:19, minute:10, timezone:'+02:30'})",
                datetime(2022, 3, 2, 19, 10, 0, 0, ZoneOffset.ofHoursMinutes(2, 30)));
    }

    @ParameterizedTest(name = "{displayName} {0}")
    @MethodSource("factoryProvider")
    public void shouldSendAndReceiveDateTimeWithTimeZoneOffset(TransportConnection.Factory connectionFactory)
            throws Exception {
        initConnection(connectionFactory);

        testSendingAndReceivingOfBoltV2Value(datetime(1899, 1, 1, 12, 12, 32, 0, ZoneOffset.ofHoursMinutes(-4, -15)));
    }

    private <T extends AnyValue> void testSendingOfBoltV2Value(T value) throws Exception {
        handshakeAndAuth();

        connection
                .send(run("CREATE (n:Node {value: $value}) RETURN 42", map(new String[] {"value"}, new AnyValue[] {value
                })))
                .send(pull());

        assertThat(connection).receivesSuccess().receivesRecord(longValue(42)).receivesSuccess();
    }

    private <T extends AnyValue> void testReceivingOfBoltV2Value(String query, T expectedValue) throws Exception {
        handshakeAndAuth();

        connection.send(run(query)).send(pull());

        assertThat(connection).receivesSuccess().receivesRecord(expectedValue).receivesSuccess();
    }

    private <T extends AnyValue> void testSendingAndReceivingOfBoltV2Value(T value) throws Exception {
        handshakeAndAuth();

        connection
                .send(run("RETURN $value", map(new String[] {"value"}, new AnyValue[] {value})))
                .send(pull());

        assertThat(connection).receivesSuccess().receivesRecord(value).receivesSuccess();
    }

    private void handshakeAndAuth() throws Exception {
        connection.connect().sendDefaultProtocolVersion().send(hello());

        assertThat(connection).negotiatesDefaultVersion().receivesSuccess();
    }
}
