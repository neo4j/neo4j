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
package org.neo4j.bolt.v50.runtime;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.neo4j.bolt.testing.assertions.BoltConnectionAssertions.assertThat;
import static org.neo4j.packstream.testing.PackstreamBufAssertions.assertThat;
import static org.neo4j.values.storable.Values.longValue;

import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Map;
import java.util.function.Consumer;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.neo4j.bolt.AbstractBoltITBase;
import org.neo4j.bolt.protocol.common.connector.connection.Feature;
import org.neo4j.bolt.testing.client.TransportConnection;
import org.neo4j.bolt.testing.messages.BoltV44Wire;
import org.neo4j.bolt.testing.messages.BoltV50Wire;
import org.neo4j.bolt.testing.messages.BoltWire;
import org.neo4j.bolt.transport.Neo4jWithSocketExtension;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.packstream.error.reader.UnexpectedTypeException;
import org.neo4j.packstream.io.PackstreamBuf;
import org.neo4j.test.extension.testdirectory.EphemeralTestDirectoryExtension;
import org.neo4j.values.storable.DateTimeValue;
import org.neo4j.values.virtual.MapValueBuilder;

@EphemeralTestDirectoryExtension
@Neo4jWithSocketExtension
public class BoltV50TransportIT extends AbstractBoltITBase {

    @Override
    protected BoltWire initWire() {
        return new BoltV50Wire();
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("argumentsProvider")
    void shouldReturnElementIdForNodes(TransportConnection.Factory connectionFactory) throws Exception {
        this.connectAndHandshake(connectionFactory);

        connection
                .send(wire.run("CREATE (m:Movie{title:\"The Matrix\"}) RETURN m"))
                .send(wire.pull());

        assertThat(connection)
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
        assertThat(buf)
                .containsInt(0)
                .satisfies(legacyNodeIdAssertions)
                .containsString("PLAYED_IN")
                .containsMap(properties ->
                        Assertions.assertThat(properties).hasSize(1).containsEntry("year", 2021L))
                .containsString(elementId -> Assertions.assertThat(elementId).isNotBlank())
                .satisfies(nodeIdAssertions);
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("argumentsProvider")
    void shouldReturnElementIdForRelationships(TransportConnection.Factory connectionFactory) throws Exception {
        this.connectAndHandshake(connectionFactory);

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
                        .containsStruct(0x52, 8)
                        .satisfies(buf -> assertElementIdRelationship(
                                buf, b -> assertThat(b).containsInt(0).containsInt(1), b -> assertThat(b)
                                        .containsString(startNodeElementId -> Assertions.assertThat(startNodeElementId)
                                                .isNotBlank())
                                        .containsString(endNodeElementId -> Assertions.assertThat(endNodeElementId)
                                                .isNotBlank())))
                        .asBuffer()
                        .hasNoRemainingReadableBytes())
                .receivesSuccess();
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("argumentsProvider")
    void shouldReturnElementIdForPaths(TransportConnection.Factory connectionFactory) throws Exception {
        this.connectAndHandshake(connectionFactory);

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

    @ParameterizedTest(name = "{0}")
    @MethodSource("argumentsProvider")
    void shouldReturnUniqueNodeIds(TransportConnection.Factory connectionFactory) throws Exception {
        this.connectAndHandshake(connectionFactory);

        connection
                .send(wire.run("CREATE (a), (b), (c), (d), (e) RETURN a,b,c,d,e"))
                .send(wire.pull());

        assertThat(connection)
                .receivesSuccess()
                .packstreamSatisfies(stream -> stream.receivesMessage()
                        .containsStruct(0x71, 1)
                        .containsListHeader(5)
                        .satisfies(BoltV50TransportIT::assertUniqueNodeIdsReturned)
                        .asBuffer()
                        .hasNoRemainingReadableBytes())
                .receivesSuccess();
    }

    /**
     * Ensure that 5.x drivers cannot negotiate the UTC patch as it is implicitly enabled as part of the 5.0 protocol.
     */
    @ParameterizedTest
    @MethodSource("argumentsProvider")
    void shouldFailToNegotiateUTCPatch(TransportConnection.Factory connectionFactory) throws Exception {
        this.connectAndNegotiate(connectionFactory);

        wire.enable(Feature.UTC_DATETIME);

        connection.send(wire.hello());

        assertThat(connection)
                .receivesSuccess(meta -> Assertions.assertThat(meta).doesNotContainKey("patch_bolt"));
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("argumentsProvider")
    void shouldRejectLegacyOffsetDates(TransportConnection.Factory connectionFactory) throws Exception {
        this.connectAndHandshake(connectionFactory);

        var input =
                DateTimeValue.datetime(OffsetDateTime.of(1995, 6, 14, 12, 50, 35, 556000000, ZoneOffset.ofHours(1)));

        var params = new MapValueBuilder();
        params.add("input", input);

        var legacyOffsetWire = new BoltV44Wire();

        connection.send(legacyOffsetWire.run("RETURN $input", params.build()));

        assertThat(connection).receivesFailure(Status.Request.Invalid);
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("argumentsProvider")
    void shouldImplicitlyAcceptUTCOffsetDateTimes(TransportConnection.Factory connectionFactory) throws Exception {
        this.connectAndHandshake(connectionFactory);

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
                        .containsStruct(0x49, 3)
                        .containsInt(803130635)
                        .containsInt(556000000)
                        .containsInt(3600)
                        .asBuffer()
                        .hasNoRemainingReadableBytes())
                .receivesSuccess();
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("argumentsProvider")
    void shouldImplicitlyAcceptUTCZoneDateTimes(TransportConnection.Factory connectionFactory) throws Exception {
        this.connectAndHandshake(connectionFactory);

        connection
                .send(wire.run("RETURN datetime('1995-06-14T12:50:35.556+02:00[Europe/Berlin]')"))
                .send(wire.pull());

        assertThat(connection)
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

    @ParameterizedTest(name = "{0}")
    @MethodSource("argumentsProvider")
    void shouldAcceptTransactionType(TransportConnection.Factory connectionFactory) throws Exception {
        this.connectAndHandshake(connectionFactory);

        connection
                .send(wire.begin("neo4j", null, null, "implicit"))
                .send(wire.run("RETURN 1"))
                .send(wire.pull())
                .send(wire.commit())
                .send(wire.begin("neo4j", null, null, "nonsense"))
                .send(wire.run("RETURN 1"))
                .send(wire.pull())
                .send(wire.commit());

        assertThat(connection)
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

    private static void assertUniqueNodeIdsReturned(PackstreamBuf buf) {
        var seenNode = new ArrayList<Long>();
        for (int i = 0; i < 5; i++) {
            try {
                assertTrue(seenNode.add(assertNode(buf)));
            } catch (UnexpectedTypeException e) {
                fail(e);
            }
        }
    }

    private static long assertNode(PackstreamBuf buf) throws UnexpectedTypeException {
        assertThat(buf).containsStruct(0x4E, 4);
        long nodeId = buf.readInt();
        assertThat(buf)
                .containsList(label -> Assertions.assertThat(label).isEmpty())
                .containsMap(propMap -> Assertions.assertThat(propMap).isEmpty())
                .containsString(elementId -> Assertions.assertThat(elementId).isNotBlank());
        return nodeId;
    }

    private static void assertElementIdNode(
            PackstreamBuf buf, long nodeId, String label, Consumer<Map<String, Object>> propertyAssertions) {
        assertThat(buf)
                .containsStruct(0x4E, 4)
                .containsInt(nodeId)
                .containsList(labels -> Assertions.assertThat(labels).containsExactly(label))
                .containsMap(propertyAssertions)
                .containsString(elementId -> Assertions.assertThat(elementId).isNotBlank());
    }
}
