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

import static org.neo4j.values.storable.Values.longValue;

import java.io.IOException;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Map;
import java.util.function.Consumer;
import org.assertj.core.api.Assertions;
import org.neo4j.bolt.protocol.common.connector.connection.Feature;
import org.neo4j.bolt.test.annotation.BoltTestExtension;
import org.neo4j.bolt.test.annotation.connection.initializer.Authenticated;
import org.neo4j.bolt.test.annotation.connection.initializer.VersionSelected;
import org.neo4j.bolt.test.annotation.test.ProtocolTest;
import org.neo4j.bolt.test.annotation.wire.initializer.EnableFeature;
import org.neo4j.bolt.test.annotation.wire.selector.ExcludeWire;
import org.neo4j.bolt.testing.annotation.Version;
import org.neo4j.bolt.testing.assertions.BoltConnectionAssertions;
import org.neo4j.bolt.testing.client.TransportConnection;
import org.neo4j.bolt.testing.messages.BoltV44Wire;
import org.neo4j.bolt.testing.messages.BoltWire;
import org.neo4j.bolt.transport.Neo4jWithSocketExtension;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.packstream.error.reader.UnexpectedTypeException;
import org.neo4j.packstream.io.PackstreamBuf;
import org.neo4j.packstream.testing.PackstreamBufAssertions;
import org.neo4j.test.extension.testdirectory.EphemeralTestDirectoryExtension;
import org.neo4j.values.storable.DateTimeValue;
import org.neo4j.values.storable.Values;
import org.neo4j.values.virtual.MapValueBuilder;

/**
 * Evaluates whether Bolt correctly streams results.
 */
@EphemeralTestDirectoryExtension
@Neo4jWithSocketExtension
@BoltTestExtension
public class StreamingIT {

    @ProtocolTest
    void shouldStreamWhenStatementIdNotProvided(BoltWire wire, @Authenticated TransportConnection connection)
            throws IOException {
        // begin a transaction
        connection.send(wire.begin());
        BoltConnectionAssertions.assertThat(connection).receivesSuccess();

        // execute a query
        connection.send(wire.run("UNWIND range(30, 40) AS x RETURN x"));
        BoltConnectionAssertions.assertThat(connection)
                .receivesSuccess(meta ->
                        Assertions.assertThat(meta).containsEntry("qid", 0L).containsKeys("fields", "t_first"));

        // request 5 records but do not provide qid
        connection.send(wire.pull(5));
        BoltConnectionAssertions.assertThat(connection)
                .receivesRecord(Values.longValue(30L))
                .receivesRecord(Values.longValue(31L))
                .receivesRecord(Values.longValue(32L))
                .receivesRecord(Values.longValue(33L))
                .receivesRecord(Values.longValue(34L))
                .receivesSuccess(meta -> Assertions.assertThat(meta).containsEntry("has_more", true));

        // request 2 more records but do not provide qid
        connection.send(wire.pull(2));
        BoltConnectionAssertions.assertThat(connection)
                .receivesRecord(Values.longValue(35L))
                .receivesRecord(Values.longValue(36L))
                .receivesSuccess(meta -> Assertions.assertThat(meta).containsEntry("has_more", true));

        // request 3 more records and provide qid
        connection.send(wire.pull(3L, 0));

        BoltConnectionAssertions.assertThat(connection)
                .receivesRecord(Values.longValue(37L))
                .receivesRecord(Values.longValue(38L))
                .receivesRecord(Values.longValue(39L))
                .receivesSuccess(meta -> Assertions.assertThat(meta).containsEntry("has_more", true));

        // request 10 more records but do not provide qid, only 1 more record is available
        connection.send(wire.pull(10L));
        BoltConnectionAssertions.assertThat(connection)
                .receivesRecord(Values.longValue(40L))
                .receivesSuccess(meta -> Assertions.assertThat(meta).containsKey("t_last"));

        // rollback the transaction
        connection.send(wire.rollback());
        BoltConnectionAssertions.assertThat(connection).receivesSuccess();
    }

    @ProtocolTest
    void shouldSendAndReceiveStatementIds(BoltWire wire, @Authenticated TransportConnection connection)
            throws IOException {
        // begin a transaction
        connection.send(wire.begin());
        BoltConnectionAssertions.assertThat(connection).receivesSuccess();

        // execute query #0
        connection.send(wire.run("UNWIND range(1, 10) AS x RETURN x"));
        BoltConnectionAssertions.assertThat(connection)
                .receivesSuccess(meta ->
                        Assertions.assertThat(meta).containsEntry("qid", 0L).containsKeys("fields", "t_first"));

        // request 3 records for query #0
        connection.send(wire.pull(3L, 0));
        BoltConnectionAssertions.assertThat(connection)
                .receivesRecord(Values.longValue(1L))
                .receivesRecord(Values.longValue(2L))
                .receivesRecord(Values.longValue(3L))
                .receivesSuccess(meta -> Assertions.assertThat(meta).containsEntry("has_more", true));

        // execute query #1
        connection.send(wire.run("UNWIND range(11, 20) AS x RETURN x"));
        BoltConnectionAssertions.assertThat(connection)
                .receivesSuccess(meta ->
                        Assertions.assertThat(meta).containsEntry("qid", 1L).containsKeys("fields", "t_first"));

        // request 2 records for query #1
        connection.send(wire.pull(2, 1));
        BoltConnectionAssertions.assertThat(connection)
                .receivesRecord(Values.longValue(11L))
                .receivesRecord(Values.longValue(12L))
                .receivesSuccess(meta -> Assertions.assertThat(meta).containsEntry("has_more", true));

        // execute query #2
        connection.send(wire.run("UNWIND range(21, 30) AS x RETURN x"));
        BoltConnectionAssertions.assertThat(connection)
                .receivesSuccess(meta ->
                        Assertions.assertThat(meta).containsEntry("qid", 2L).containsKeys("fields", "t_first"));

        // request 4 records for query #2
        // no qid - should use the statement from the latest RUN
        connection.send(wire.pull(4));

        BoltConnectionAssertions.assertThat(connection)
                .receivesRecord(Values.longValue(21L))
                .receivesRecord(Values.longValue(22L))
                .receivesRecord(Values.longValue(23L))
                .receivesRecord(Values.longValue(24L))
                .receivesSuccess(meta -> Assertions.assertThat(meta).containsEntry("has_more", true));

        // execute query #3
        connection.send(wire.run("UNWIND range(31, 40) AS x RETURN x"));
        BoltConnectionAssertions.assertThat(connection)
                .receivesSuccess(meta ->
                        Assertions.assertThat(meta).containsEntry("qid", 3L).containsKeys("fields", "t_first"));

        // request 1 record for query #3
        connection.send(wire.pull(1, 3));
        BoltConnectionAssertions.assertThat(connection)
                .receivesRecord(Values.longValue(31L))
                .receivesSuccess(meta -> Assertions.assertThat(meta).containsEntry("has_more", true));

        // request 2 records for query #0
        connection.send(wire.pull(2, 0));
        BoltConnectionAssertions.assertThat(connection)
                .receivesRecord(Values.longValue(4L))
                .receivesRecord(Values.longValue(5L))
                .receivesSuccess(meta -> Assertions.assertThat(meta).containsEntry("has_more", true));

        // request 9 records for query #3
        connection.send(wire.pull(9, 3));
        BoltConnectionAssertions.assertThat(connection)
                .receivesRecord(Values.longValue(32L))
                .receivesRecord(Values.longValue(33L))
                .receivesRecord(Values.longValue(34L))
                .receivesRecord(Values.longValue(35L))
                .receivesRecord(Values.longValue(36L))
                .receivesRecord(Values.longValue(37L))
                .receivesRecord(Values.longValue(38L))
                .receivesRecord(Values.longValue(39L))
                .receivesRecord(Values.longValue(40L))
                .receivesSuccess(meta ->
                        Assertions.assertThat(meta).containsKey("t_last").doesNotContainKey("has_more"));

        // commit the transaction
        connection.send(wire.commit());
        BoltConnectionAssertions.assertThat(connection).receivesSuccess();
    }

    @ProtocolTest
    void shouldAcceptTransactionType(BoltWire wire, @Authenticated TransportConnection connection) throws Exception {
        connection
                .send(wire.begin(x -> x.withDatabase("neo4j").withTransactionType("implicit")))
                .send(wire.run("RETURN 1"))
                .send(wire.pull())
                .send(wire.commit())
                .send(wire.begin(x -> x.withDatabase("neo4j").withTransactionType("nonsense")))
                .send(wire.run("RETURN 1"))
                .send(wire.pull())
                .send(wire.commit());

        BoltConnectionAssertions.assertThat(connection)
                .receivesSuccess()
                .receivesSuccess()
                .receivesRecord(longValue(1))
                .receivesSuccess()
                .receivesSuccess()
                .receivesSuccess()
                .receivesSuccess()
                .receivesRecord(longValue(1))
                .receivesSuccess()
                .receivesSuccess();
    }

    @ProtocolTest
    void shouldReturnDatabaseNameOnCompletionViaPull(BoltWire wire, @Authenticated TransportConnection connection)
            throws IOException {
        connection.send(wire.begin());
        BoltConnectionAssertions.assertThat(connection).receivesSuccess();

        connection.send(wire.run("UNWIND range(1, 10) AS x RETURN x"));
        BoltConnectionAssertions.assertThat(connection)
                .receivesSuccess(meta ->
                        Assertions.assertThat(meta).containsEntry("qid", 0L).containsKeys("fields", "t_first"));

        connection.send(wire.pull(5));
        BoltConnectionAssertions.assertThat(connection)
                .receivesSuccessAfterRecords(meta -> Assertions.assertThat(meta).doesNotContainKeys("db", "t_last"));

        connection.send(wire.pull());
        BoltConnectionAssertions.assertThat(connection)
                .receivesSuccessAfterRecords(meta ->
                        Assertions.assertThat(meta).containsEntry("db", "neo4j").containsKey("t_last"));

        connection.send(wire.rollback());
        BoltConnectionAssertions.assertThat(connection).receivesSuccess();
    }

    @ProtocolTest
    void shouldReturnDatabaseNameOnCompletionViaDiscard(BoltWire wire, @Authenticated TransportConnection connection)
            throws IOException {
        connection.send(wire.begin());
        BoltConnectionAssertions.assertThat(connection).receivesSuccess();

        connection.send(wire.run("UNWIND range(1, 10) AS x RETURN x"));
        BoltConnectionAssertions.assertThat(connection)
                .receivesSuccess(meta ->
                        Assertions.assertThat(meta).containsEntry("qid", 0L).containsKeys("fields", "t_first"));

        connection.send(wire.discard(5));
        BoltConnectionAssertions.assertThat(connection)
                .receivesSuccessAfterRecords(meta -> Assertions.assertThat(meta).doesNotContainKeys("db", "t_last"));

        connection.send(wire.discard());
        BoltConnectionAssertions.assertThat(connection)
                .receivesSuccessAfterRecords(meta ->
                        Assertions.assertThat(meta).containsEntry("db", "neo4j").containsKey("t_last"));

        connection.send(wire.rollback());
        BoltConnectionAssertions.assertThat(connection).receivesSuccess();
    }

    private static void assertElementIdNode(
            PackstreamBuf buf, long nodeId, String label, Consumer<Map<String, Object>> propertyAssertions) {
        PackstreamBufAssertions.assertThat(buf)
                .containsStruct(0x4E, 4)
                .containsInt(nodeId)
                .containsList(labels -> Assertions.assertThat(labels).containsExactly(label))
                .containsMap(propertyAssertions)
                .containsString(elementId -> Assertions.assertThat(elementId).isNotBlank());
    }

    @ProtocolTest
    @ExcludeWire(@Version(major = 4))
    void shouldReturnElementIdForNodes(BoltWire wire, @Authenticated TransportConnection connection)
            throws IOException {
        connection
                .send(wire.run("CREATE (m:Movie{title:\"The Matrix\"}) RETURN m"))
                .send(wire.pull());

        BoltConnectionAssertions.assertThat(connection)
                .receivesSuccess()
                .packstreamSatisfies(stream -> stream.receivesMessage()
                        .containsStruct(0x71, 1)
                        .containsListHeader(1)
                        .satisfies(buf ->
                                assertElementIdNode(buf, 0, "Movie", properties -> Assertions.assertThat(properties)
                                        .hasSize(1)
                                        .containsEntry("title", "The Matrix")))
                        .asBuffer()
                        .hasNoRemainingReadableBytes())
                .receivesSuccess();
    }

    private static void assertElementIdRelationship(
            PackstreamBuf buf,
            Consumer<PackstreamBuf> legacyNodeIdAssertions,
            Consumer<PackstreamBuf> nodeIdAssertions) {
        PackstreamBufAssertions.assertThat(buf)
                .containsAInt()
                .satisfies(legacyNodeIdAssertions)
                .containsString("PLAYED_IN")
                .containsMap(properties ->
                        Assertions.assertThat(properties).hasSize(1).containsEntry("year", 2021L))
                .containsString(elementId -> Assertions.assertThat(elementId).isNotBlank())
                .satisfies(nodeIdAssertions);
    }

    @ProtocolTest
    @ExcludeWire(@Version(major = 4))
    void shouldReturnElementIdForRelationships(BoltWire wire, @Authenticated TransportConnection connection)
            throws IOException {
        connection
                .send(
                        wire.run(
                                "CREATE (:Actor{name: \"Greg\"})-[r:PLAYED_IN{year: 2021}]->(:Movie{title:\"The Matrix\"}) RETURN r"))
                .send(wire.pull());

        BoltConnectionAssertions.assertThat(connection)
                .receivesSuccess()
                .packstreamSatisfies(stream -> stream.receivesMessage()
                        .containsStruct(0x71, 1)
                        .containsListHeader(1)
                        .containsStruct(0x52, 8)
                        .satisfies(buf -> assertElementIdRelationship(
                                buf,
                                b -> PackstreamBufAssertions.assertThat(b)
                                        .containsInt(0)
                                        .containsInt(1),
                                b -> PackstreamBufAssertions.assertThat(b)
                                        .containsString(startNodeElementId -> Assertions.assertThat(startNodeElementId)
                                                .isNotBlank())
                                        .containsString(endNodeElementId -> Assertions.assertThat(endNodeElementId)
                                                .isNotBlank())))
                        .asBuffer()
                        .hasNoRemainingReadableBytes())
                .receivesSuccess();
    }

    @ProtocolTest
    @ExcludeWire(@Version(major = 4))
    void shouldReturnElementIdForPaths(BoltWire wire, @Authenticated TransportConnection connection)
            throws IOException {
        connection
                .send(
                        wire.run(
                                "CREATE p=(:Actor{name: \"Greg\"})-[:PLAYED_IN{year: 2021}]->(:Movie{title:\"The Matrix\"}) RETURN p"))
                .send(wire.pull());

        BoltConnectionAssertions.assertThat(connection)
                .receivesSuccess()
                .packstreamSatisfies(stream -> stream.receivesMessage()
                        .containsStruct(0x71, 1)
                        .containsListHeader(1)
                        .containsStruct(0x50, 3)
                        .containsListHeader(2)
                        .satisfies(buf ->
                                assertElementIdNode(buf, 0, "Actor", properties -> Assertions.assertThat(properties)
                                        .hasSize(1)
                                        .containsEntry("name", "Greg")))
                        .satisfies(buf ->
                                assertElementIdNode(buf, 1, "Movie", properties -> Assertions.assertThat(properties)
                                        .hasSize(1)
                                        .containsEntry("title", "The Matrix")))
                        .containsListHeader(1)
                        .containsStruct(0x72, 4)
                        .satisfies(buf -> assertElementIdRelationship(buf, b -> {}, b -> {}))
                        .containsList(indices -> Assertions.assertThat(indices).containsExactly(1L, 1L))
                        .asBuffer()
                        .hasNoRemainingReadableBytes())
                .receivesSuccess();
    }

    @ProtocolTest
    @ExcludeWire(@Version(major = 4))
    void shouldNotNegotiateUTCPatch(BoltWire wire, @VersionSelected TransportConnection connection) throws IOException {
        wire.enable(Feature.UTC_DATETIME);
        connection.send(wire.hello());

        BoltConnectionAssertions.assertThat(connection)
                .receivesSuccess(meta -> Assertions.assertThat(meta).doesNotContainKey("patch_bolt"));
    }

    @ProtocolTest
    @EnableFeature(Feature.UTC_DATETIME)
    void shouldAcceptUTCOffsetDateTimes(BoltWire wire, @Authenticated TransportConnection connection)
            throws IOException {
        var input =
                DateTimeValue.datetime(OffsetDateTime.of(1995, 6, 14, 12, 50, 35, 556000000, ZoneOffset.ofHours(1)));

        var params = new MapValueBuilder();
        params.add("input", input);

        connection.send(wire.run("RETURN $input", params.build())).send(wire.pull());

        BoltConnectionAssertions.assertThat(connection)
                .receivesSuccess()
                .packstreamSatisfies(stream -> stream.receivesMessage()
                        .containsStruct(0x71, 1)
                        .containsListHeader(1)
                        .containsStruct(0x49, 3)
                        .containsInt(803130635)
                        .containsInt(556000000)
                        .containsInt(3600)
                        .asBuffer()
                        .hasNoRemainingReadableBytes())
                .receivesSuccess();
    }

    @ProtocolTest
    @EnableFeature(Feature.UTC_DATETIME)
    void shouldAcceptUTCZoneDateTimes(BoltWire wire, @Authenticated TransportConnection connection) throws IOException {
        connection
                .send(wire.run("RETURN datetime('1995-06-14T12:50:35.556+02:00[Europe/Berlin]')"))
                .send(wire.pull());

        BoltConnectionAssertions.assertThat(connection)
                .receivesSuccess()
                .packstreamSatisfies(stream -> stream.receivesMessage()
                        .containsStruct(0x71, 1)
                        .containsListHeader(1)
                        .containsStruct(0x69, 3)
                        .containsInt(803127035)
                        .containsInt(556000000)
                        .containsString("Europe/Berlin")
                        .asBuffer()
                        .hasNoRemainingReadableBytes())
                .receivesSuccess();
    }

    @ProtocolTest
    @EnableFeature(Feature.UTC_DATETIME)
    void shouldRejectLegacyOffsetDatesWhenUTCIsAvailable(BoltWire wire, @Authenticated TransportConnection connection)
            throws IOException {
        // switch back to a legacy wire revision in order to easily transmit an invalid struct to the server
        if (wire.getProtocolVersion().major() >= 5) {
            wire = new BoltV44Wire();
        } else {
            wire.disable(Feature.UTC_DATETIME);
        }

        var input =
                DateTimeValue.datetime(OffsetDateTime.of(1995, 6, 14, 12, 50, 35, 556000000, ZoneOffset.ofHours(1)));

        var params = new MapValueBuilder();
        params.add("input", input);

        connection.send(wire.run("RETURN $input", params.build()));

        BoltConnectionAssertions.assertThat(connection).receivesFailure(Status.Request.Invalid);
    }

    private static void assertUniqueNodeIdsReturned(PackstreamBuf buf) {
        var seenNode = new ArrayList<Long>();
        for (int i = 0; i < 5; i++) {
            try {
                Assertions.assertThat(seenNode.add(extractNodeId(buf))).isTrue();
            } catch (UnexpectedTypeException e) {
                org.junit.jupiter.api.Assertions.fail(e);
            }
        }
    }

    private static long extractNodeId(PackstreamBuf buf) throws UnexpectedTypeException {
        PackstreamBufAssertions.assertThat(buf).containsStruct(0x4E, 4);
        long nodeId = buf.readInt();

        PackstreamBufAssertions.assertThat(buf)
                .containsList(label -> Assertions.assertThat(label).isEmpty())
                .containsMap(propMap -> Assertions.assertThat(propMap).isEmpty())
                .containsString(elementId -> Assertions.assertThat(elementId).isNotBlank());

        return nodeId;
    }

    @ProtocolTest
    @ExcludeWire(@Version(major = 4))
    void shouldReturnUniqueNodeIds(BoltWire wire, @Authenticated TransportConnection connection) throws IOException {
        connection
                .send(wire.run("CREATE (a), (b), (c), (d), (e) RETURN a,b,c,d,e"))
                .send(wire.pull());

        BoltConnectionAssertions.assertThat(connection)
                .receivesSuccess()
                .packstreamSatisfies(stream -> stream.receivesMessage()
                        .containsStruct(0x71, 1)
                        .containsListHeader(5)
                        .satisfies(StreamingIT::assertUniqueNodeIdsReturned)
                        .asBuffer()
                        .hasNoRemainingReadableBytes())
                .receivesSuccess();
    }
}
