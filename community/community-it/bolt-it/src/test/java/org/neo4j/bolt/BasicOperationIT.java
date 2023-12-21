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

import static org.assertj.core.api.InstanceOfAssertFactories.list;
import static org.neo4j.bolt.testing.assertions.BoltConnectionAssertions.assertThat;
import static org.neo4j.values.storable.Values.longValue;
import static org.neo4j.values.storable.Values.stringValue;

import java.io.IOException;
import org.assertj.core.api.Assertions;
import org.neo4j.bolt.test.annotation.BoltTestExtension;
import org.neo4j.bolt.test.annotation.connection.initializer.Authenticated;
import org.neo4j.bolt.test.annotation.test.ProtocolTest;
import org.neo4j.bolt.testing.assertions.BoltConnectionAssertions;
import org.neo4j.bolt.testing.client.TransportConnection;
import org.neo4j.bolt.testing.messages.BoltWire;
import org.neo4j.bolt.transport.Neo4jWithSocketExtension;
import org.neo4j.graphdb.SeverityLevel;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.packstream.io.Type;
import org.neo4j.packstream.testing.PackstreamBufAssertions;
import org.neo4j.test.extension.testdirectory.EphemeralTestDirectoryExtension;

@EphemeralTestDirectoryExtension
@Neo4jWithSocketExtension
@BoltTestExtension
public class BasicOperationIT {

    /**
     * Defines a pattern via which the elementId values returned by nodes and relationships are validated.
     * <p />
     * Typically, we do not validate the contents of values returned by bolt, however, we want to make sure that the
     * returned value at least makes sense and hasn't somehow been truncated somewhere within the pipeline.
     */
    public static final String ELEMENT_ID_PATTERN =
            "\\d+:[\\da-f]{8}\\-[\\da-f]{4}\\-[\\da-f]{4}\\-[\\da-f]{4}\\-[\\da-f]{12}:\\d+";

    @ProtocolTest
    void shouldRunSimpleStatement(BoltWire wire, @Authenticated TransportConnection connection) throws IOException {
        // When
        connection
                .send(wire.run("UNWIND [1,2,3] AS a RETURN a, a * a AS a_squared"))
                .send(wire.pull());

        // Then
        assertThat(connection)
                .receivesSuccess(meta -> Assertions.assertThat(meta)
                        .containsKey("t_first")
                        .hasEntrySatisfying("fields", fields -> Assertions.assertThat(fields)
                                .asInstanceOf(list(String.class))
                                .hasSize(2)
                                .containsExactly("a", "a_squared")))
                .receivesRecord(longValue(1), longValue(1))
                .receivesRecord(longValue(2), longValue(4))
                .receivesRecord(longValue(3), longValue(9))
                .receivesSuccess(meta ->
                        Assertions.assertThat(meta).containsKey("t_last").containsEntry("type", "r"));
    }

    @ProtocolTest
    void shouldRespondWithMetadataToDiscardAll(BoltWire wire, @Authenticated TransportConnection connection)
            throws IOException {
        // When
        connection
                .send(wire.run("UNWIND [1,2,3] AS a RETURN a, a * a AS a_squared"))
                .send(wire.discard());

        // Then
        BoltConnectionAssertions.assertThat(connection)
                .receivesSuccess(meta -> Assertions.assertThat(meta)
                        .containsKey("t_first")
                        .hasEntrySatisfying("fields", fields -> Assertions.assertThat(fields)
                                .asInstanceOf(list(String.class))
                                .hasSize(2)
                                .containsExactly("a", "a_squared")))
                .receivesSuccess(meta ->
                        Assertions.assertThat(meta).containsKey("t_last").containsEntry("type", "r"));
    }

    @ProtocolTest
    void shouldBeAbleToRunQueryAfterAckFailure(BoltWire wire, @Authenticated TransportConnection connection)
            throws IOException {
        // Given
        connection.send(wire.run("QINVALID")).send(wire.pull());

        assertThat(connection)
                .receivesFailureFuzzy(Status.Statement.SyntaxError, "line 1, column 1")
                .receivesIgnored();

        // When
        connection.send(wire.reset()).send(wire.run("RETURN 1")).send(wire.pull());

        // Then
        assertThat(connection)
                .receivesSuccess()
                .receivesSuccess()
                .receivesRecord(longValue(1))
                .receivesSuccess();
    }

    @ProtocolTest
    void shouldRunProcedure(BoltWire wire, @Authenticated TransportConnection connection) throws IOException {
        // Given
        connection
                .send(wire.run("CREATE (n:Test {age: 2}) RETURN n.age AS age"))
                .send(wire.pull());

        assertThat(connection)
                .receivesSuccess(meta -> Assertions.assertThat(meta)
                        .containsKey("t_first")
                        .hasEntrySatisfying("fields", fields -> Assertions.assertThat(fields)
                                .asInstanceOf(list(String.class))
                                .hasSize(1)
                                .containsExactly("age")))
                .receivesRecord(longValue(2))
                .receivesSuccess(meta -> Assertions.assertThat(meta).containsKey("t_last"));

        // When
        connection.send(wire.run("CALL db.labels() YIELD label")).send(wire.pull());

        // Then
        assertThat(connection)
                .receivesSuccess(meta -> Assertions.assertThat(meta)
                        .containsKey("t_first")
                        .hasEntrySatisfying("fields", fields -> Assertions.assertThat(fields)
                                .asInstanceOf(list(String.class))
                                .hasSize(1)
                                .containsExactly("label")))
                .receivesRecord(stringValue("Test"))
                .receivesSuccess();
    }

    @ProtocolTest
    void shouldHandleDeletedNodes(BoltWire wire, @Authenticated TransportConnection connection) throws IOException {
        // When
        connection.send(wire.run("CREATE (n:Test) DELETE n RETURN n")).send(wire.pull());

        // Then
        assertThat(connection)
                .receivesSuccess(meta -> Assertions.assertThat(meta)
                        .containsKey("t_first")
                        .hasEntrySatisfying("fields", fields -> Assertions.assertThat(fields)
                                .asInstanceOf(list(String.class))
                                .hasSize(1)
                                .containsExactly("n")))
                .packstreamSatisfies(pack -> pack.receivesMessage()
                        // Record(0x71) {
                        //  fields: [
                        //      Node(0x4E) {
                        //          id: 00
                        //          labels: [] (90)
                        //          props: {} (A)
                        //      }
                        //  ]
                        // }
                        .containsStruct(0x71, 1)
                        .containsLengthPrefixMarker(Type.LIST, 1)
                        // In legacy protocol versions, elementId fields are omitted
                        .containsStruct(0x4E, wire.getProtocolVersion().major() >= 5 ? 4 : 3)
                        .containsInt(0)
                        .containsList(labels -> Assertions.assertThat(labels).isEmpty())
                        .containsMap(props -> Assertions.assertThat(props).isEmpty())
                        .satisfies(buf -> {
                            if (wire.getProtocolVersion().major() < 5) {
                                return;
                            }

                            PackstreamBufAssertions.assertThat(buf)
                                    .containsString(elementId ->
                                            Assertions.assertThat(elementId).matches(ELEMENT_ID_PATTERN));
                        })
                        .asBuffer()
                        .hasNoRemainingReadableBytes())
                .receivesSuccess(meta -> Assertions.assertThat(meta).containsKey("t_last"));
    }

    @ProtocolTest
    void shouldHandleDeletedRelationships(BoltWire wire, @Authenticated TransportConnection connection)
            throws IOException {
        // When
        connection
                .send(wire.run("CREATE (a)-[r:T {prop: 42}]->(b) DELETE r RETURN r"))
                .send(wire.pull());

        // Then
        assertThat(connection)
                .receivesSuccess(meta -> Assertions.assertThat(meta)
                        .containsKey("t_first")
                        .hasEntrySatisfying("fields", fields -> Assertions.assertThat(fields)
                                .asInstanceOf(list(String.class))
                                .hasSize(1)
                                .containsExactly("r")))
                .packstreamSatisfies(pack -> pack.receivesMessage()
                        // Record(0x71) {
                        //  fields: [
                        //      Relationship(0x52) {
                        //          relId: 00
                        //          startId: -01
                        //          endId: -01
                        //          type: "" (80)
                        //          props: {} (A0)
                        //      }
                        //  ]
                        // }
                        .containsStruct(0x71, 1)
                        .containsLengthPrefixMarker(Type.LIST, 1)
                        // Legacy relationships do not include elementId fields
                        .containsStruct(0x52, wire.getProtocolVersion().major() >= 5 ? 8 : 5)
                        .containsAInt() // This is the relationship id
                        .containsInt(-1)
                        .containsInt(-1)
                        .containsString("")
                        .containsMap(props -> Assertions.assertThat(props).isEmpty())
                        .satisfies(buf -> {
                            if (wire.getProtocolVersion().major() < 5) {
                                return;
                            }

                            PackstreamBufAssertions.assertThat(buf)
                                    .containsString(elementId ->
                                            Assertions.assertThat(elementId).matches(ELEMENT_ID_PATTERN))
                                    .containsString("")
                                    .containsString("");
                        })
                        .asBuffer()
                        .hasNoRemainingReadableBytes())
                .receivesSuccess();
    }

    @ProtocolTest
    void shouldNotLeakStatsToNextStatement(BoltWire wire, @Authenticated TransportConnection connection)
            throws IOException {
        // Given
        connection.send(wire.run("CREATE (n)")).send(wire.pull());

        assertThat(connection).receivesSuccess(2);

        // When
        connection.send(wire.run("RETURN 1")).send(wire.pull());

        // Then
        assertThat(connection)
                .receivesSuccess()
                .receivesRecord(longValue(1))
                .receivesSuccess(meta ->
                        Assertions.assertThat(meta).containsKey("t_last").containsEntry("type", "r"));
    }

    @ProtocolTest
    void shouldSendNotifications(BoltWire wire, @Authenticated TransportConnection connection) throws IOException {
        // When
        connection
                .send(wire.run("EXPLAIN MATCH (a:THIS_IS_NOT_A_LABEL) RETURN count(*)"))
                .send(wire.pull());

        // Then
        assertThat(connection)
                .receivesSuccess()
                .receivesSuccessWithNotification(
                        "Neo.ClientNotification.Statement.UnknownLabelWarning",
                        "The provided label is not in the database.",
                        "One of the labels in your query is not available in the database, "
                                + "make sure you didn't misspell it or that the label is available when "
                                + "you run this statement in your application (the missing label name is: "
                                + "THIS_IS_NOT_A_LABEL)",
                        SeverityLevel.WARNING,
                        17,
                        1,
                        18);
    }

    @ProtocolTest
    void shouldFailNicelyWhenDroppingUnknownIndex(BoltWire wire, @Authenticated TransportConnection connection)
            throws IOException {
        // When
        connection.send(wire.run("DROP INDEX my_index")).send(wire.pull());

        // Then
        assertThat(connection)
                .receivesFailure(
                        Status.Schema.IndexDropFailed,
                        "Unable to drop index called `my_index`. There is no such index.")
                .receivesIgnored();
    }

    @ProtocolTest
    void shouldFailNicelyWhenSubmittingInvalidStatement(BoltWire wire, @Authenticated TransportConnection connection)
            throws IOException {
        connection.send(wire.run("MATCH (:Movie{title:'"));

        assertThat(connection).receivesFailureFuzzy(Status.Statement.SyntaxError, "Failed to parse string literal");
    }
}
