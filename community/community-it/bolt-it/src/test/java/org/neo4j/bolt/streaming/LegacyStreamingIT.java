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
package org.neo4j.bolt.streaming;

import static org.neo4j.bolt.testing.assertions.BoltConnectionAssertions.assertThat;

import java.io.IOException;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import org.assertj.core.api.Assertions;
import org.neo4j.bolt.protocol.common.connector.connection.Feature;
import org.neo4j.bolt.test.annotation.BoltTestExtension;
import org.neo4j.bolt.test.annotation.connection.initializer.Authenticated;
import org.neo4j.bolt.test.annotation.connection.initializer.VersionSelected;
import org.neo4j.bolt.test.annotation.test.ProtocolTest;
import org.neo4j.bolt.test.annotation.wire.selector.IncludeWire;
import org.neo4j.bolt.testing.annotation.Version;
import org.neo4j.bolt.testing.client.TransportConnection;
import org.neo4j.bolt.testing.messages.BoltV44Wire;
import org.neo4j.bolt.testing.messages.BoltWire;
import org.neo4j.bolt.transport.Neo4jWithSocketExtension;
import org.neo4j.packstream.io.PackstreamBuf;
import org.neo4j.packstream.testing.PackstreamBufAssertions;
import org.neo4j.test.extension.testdirectory.EphemeralTestDirectoryExtension;
import org.neo4j.values.storable.DateTimeValue;
import org.neo4j.values.virtual.MapValueBuilder;

/**
 * Ensures that Bolt correctly streams results on legacy versions.
 */
@EphemeralTestDirectoryExtension
@Neo4jWithSocketExtension
@BoltTestExtension
@IncludeWire(@Version(major = 4))
public class LegacyStreamingIT {

    private static void assertLegacyNode(
            PackstreamBuf buf, long nodeId, String label, Consumer<Map<String, Object>> propertyAssertions) {
        PackstreamBufAssertions.assertThat(buf)
                .containsStruct(0x4E, 3)
                .containsInt(nodeId)
                .containsList(labels -> Assertions.assertThat(labels).containsExactly(label))
                .containsMap(propertyAssertions);
    }

    @ProtocolTest
    void shouldReturnLegacyIdForNodes(BoltWire wire, @Authenticated TransportConnection connection) throws IOException {
        connection
                .send(wire.run("CREATE (m:Movie{title:\"The Matrix\"}) RETURN m"))
                .send(wire.pull());

        assertThat(connection)
                .receivesSuccess()
                .packstreamSatisfies(stream -> stream.receivesMessage()
                        .containsStruct(0x71, 1)
                        .containsListHeader(1)
                        .satisfies(
                                buf -> assertLegacyNode(buf, 0, "Movie", properties -> Assertions.assertThat(properties)
                                        .hasSize(1)
                                        .containsEntry("title", "The Matrix")))
                        .asBuffer()
                        .hasNoRemainingReadableBytes())
                .receivesSuccess();
    }

    private static void assertLegacyIdRelationship(PackstreamBuf buf, Consumer<PackstreamBuf> nodeIdAssertions) {
        PackstreamBufAssertions.assertThat(buf)
                .containsAInt()
                .satisfies(nodeIdAssertions)
                .containsString("PLAYED_IN")
                .containsMap(properties ->
                        Assertions.assertThat(properties).hasSize(1).containsEntry("year", 2021L));
    }

    @ProtocolTest
    void shouldReturnLegacyIdForRelationships(BoltWire wire, @Authenticated TransportConnection connection)
            throws IOException {
        connection
                .send(
                        wire.run(
                                "CREATE (:Actor{name: \"Greg\"})-[r:PLAYED_IN{year: 2021}]->(:Movie{title:\"The Matrix\"}) RETURN r"))
                .send(wire.pull());

        assertThat(connection)
                .receivesSuccess()
                .packstreamSatisfies(stream -> stream.receivesMessage()
                        .containsStruct(0x71, 1)
                        .containsListHeader(1)
                        .containsStruct(0x52, 5)
                        .satisfies(buf -> assertLegacyIdRelationship(buf, b -> PackstreamBufAssertions.assertThat(b)
                                .containsInt(0)
                                .containsInt(1)))
                        .asBuffer()
                        .hasNoRemainingReadableBytes())
                .receivesSuccess();
    }

    @ProtocolTest
    void shouldReturnLegacyIdForPaths(BoltWire wire, @Authenticated TransportConnection connection) throws IOException {
        connection
                .send(
                        wire.run(
                                "CREATE p=(:Actor{name: \"Greg\"})-[:PLAYED_IN{year: 2021}]->(:Movie{title:\"The Matrix\"}) RETURN p"))
                .send(wire.pull());

        assertThat(connection)
                .receivesSuccess()
                .packstreamSatisfies(stream -> stream.receivesMessage()
                        .containsStruct(0x71, 1)
                        .containsListHeader(1)
                        .containsStruct(0x50, 3)
                        .containsListHeader(2)
                        .satisfies(
                                buf -> assertLegacyNode(buf, 0, "Actor", properties -> Assertions.assertThat(properties)
                                        .hasSize(1)
                                        .containsEntry("name", "Greg")))
                        .satisfies(
                                buf -> assertLegacyNode(buf, 1, "Movie", properties -> Assertions.assertThat(properties)
                                        .hasSize(1)
                                        .containsEntry("title", "The Matrix")))
                        .containsListHeader(1)
                        .containsStruct(0x72, 3)
                        .satisfies(buf -> assertLegacyIdRelationship(buf, b -> {}))
                        .containsList(indices -> Assertions.assertThat(indices).containsExactly(1L, 1L))
                        .asBuffer()
                        .hasNoRemainingReadableBytes())
                .receivesSuccess();
    }

    @ProtocolTest
    void shouldNegotiateUTCPatch(BoltWire wire, @VersionSelected TransportConnection connection) throws IOException {
        wire.enable(Feature.UTC_DATETIME);
        connection.send(wire.hello());

        assertThat(connection).receivesSuccess(meta -> Assertions.assertThat(meta)
                .containsEntry("patch_bolt", List.of(Feature.UTC_DATETIME.getId())));
    }

    @ProtocolTest
    void shouldAcceptLegacyOffsetDateTimes(BoltWire wire, @Authenticated TransportConnection connection)
            throws IOException {
        var input =
                DateTimeValue.datetime(OffsetDateTime.of(1995, 6, 14, 12, 50, 35, 556000000, ZoneOffset.ofHours(1)));

        var params = new MapValueBuilder();
        params.add("input", input);

        connection.send(wire.run("RETURN $input", params.build())).send(wire.pull());

        assertThat(connection)
                .receivesSuccess()
                .packstreamSatisfies(stream -> stream.receivesMessage()
                        .containsStruct(0x71, 1)
                        .containsListHeader(1)
                        .containsStruct(0x46, 3)
                        .containsInt(803134235)
                        .containsInt(556000000)
                        .containsInt(3600)
                        .asBuffer()
                        .hasNoRemainingReadableBytes())
                .receivesSuccess();
    }

    @ProtocolTest
    void shouldAcceptLegacyZoneDateTimes(BoltWire wire, @Authenticated TransportConnection connection)
            throws IOException {
        connection
                .send(wire.run("RETURN datetime('1995-06-14T12:50:35.556+02:00[Europe/Berlin]')"))
                .send(wire.pull());

        assertThat(connection)
                .receivesSuccess()
                .packstreamSatisfies(stream -> stream.receivesMessage()
                        .containsStruct(0x71, 1)
                        .containsListHeader(1)
                        .containsStruct(0x66, 3)
                        .containsInt(803134235)
                        .containsInt(556000000)
                        .containsString("Europe/Berlin")
                        .asBuffer()
                        .hasNoRemainingReadableBytes())
                .receivesSuccess();
    }

    @ProtocolTest
    void shouldNotAcceptUTCDatesWithoutPatch(BoltWire wire, @Authenticated TransportConnection connection)
            throws Exception {
        wire.enable(Feature.UTC_DATETIME);

        var input =
                DateTimeValue.datetime(OffsetDateTime.of(1995, 6, 14, 12, 50, 35, 556000000, ZoneOffset.ofHours(1)));

        var params = new MapValueBuilder();
        params.add("input", input);

        var utcEnabledWire = new BoltV44Wire();
        utcEnabledWire.enable(Feature.UTC_DATETIME);

        connection.send(utcEnabledWire.run("RETURN $input", params.build()));

        assertThat(connection).receivesFailure();
    }
}
