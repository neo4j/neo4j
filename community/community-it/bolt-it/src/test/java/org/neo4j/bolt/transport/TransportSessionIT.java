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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.InstanceOfAssertFactories.list;
import static org.neo4j.bolt.testing.assertions.BoltConnectionAssertions.assertThat;
import static org.neo4j.bolt.testing.client.TransportConnection.DEFAULT_PROTOCOL_VERSION;
import static org.neo4j.bolt.testing.messages.BoltDefaultWire.discard;
import static org.neo4j.bolt.testing.messages.BoltDefaultWire.pull;
import static org.neo4j.bolt.testing.messages.BoltDefaultWire.reset;
import static org.neo4j.bolt.testing.messages.BoltDefaultWire.run;
import static org.neo4j.values.storable.Values.longValue;
import static org.neo4j.values.storable.Values.stringValue;

import java.io.IOException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.neo4j.bolt.AbstractBoltTransportsTest;
import org.neo4j.bolt.negotiation.ProtocolVersion;
import org.neo4j.bolt.testing.client.TransportConnection;
import org.neo4j.graphdb.SeverityLevel;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.packstream.io.Type;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.testdirectory.EphemeralTestDirectoryExtension;

@EphemeralTestDirectoryExtension
@Neo4jWithSocketExtension
public class TransportSessionIT extends AbstractBoltTransportsTest {
    @Inject
    private Neo4jWithSocket server;

    @BeforeEach
    public void setup(TestInfo testInfo) throws IOException {
        server.setConfigure(getSettingsFunction());
        server.init(testInfo);
        address = server.lookupDefaultConnector();
    }

    @ParameterizedTest(name = "{displayName} {arguments}")
    @MethodSource("argumentsProvider")
    public void shouldNegotiateProtocolVersion(TransportConnection.Factory connectionFactory) throws Exception {
        initParameters(connectionFactory);

        // When
        connection.connect().sendDefaultProtocolVersion();

        // Then
        assertThat(connection).negotiatesDefaultVersion();
    }

    @ParameterizedTest(name = "{displayName} {arguments}")
    @MethodSource("argumentsProvider")
    public void shouldReturnNilOnNoApplicableVersion(TransportConnection.Factory connectionFactory) throws Exception {
        initParameters(connectionFactory);

        // When
        connection.connect().send(new ProtocolVersion(254, 0, 0));

        // Then
        assertThat(connection).failsToNegotiateVersion();
    }

    @ParameterizedTest(name = "{displayName} {arguments}")
    @MethodSource("argumentsProvider")
    public void shouldNegotiateOnRange(TransportConnection.Factory connectionFactory) throws Exception {
        initParameters(connectionFactory);

        var range = new ProtocolVersion(DEFAULT_PROTOCOL_VERSION.major(), 9, 9);

        connection.connect().send(range);

        assertThat(connection).negotiates(range);
    }

    @ParameterizedTest(name = "{displayName} {arguments}")
    @MethodSource("argumentsProvider")
    public void shouldNegotiateWhenPreferredIsUnavailable(TransportConnection.Factory connectionFactory)
            throws Exception {
        initParameters(connectionFactory);
        connection.connect();

        connection.send(new ProtocolVersion(ProtocolVersion.MAX_MAJOR_BIT - 1, 0, 0), DEFAULT_PROTOCOL_VERSION);

        assertThat(connection).negotiatesDefaultVersion();
    }

    @ParameterizedTest(name = "{displayName} {arguments}")
    @MethodSource("argumentsProvider")
    public void shouldRunSimpleStatement(TransportConnection.Factory connectionFactory) throws Exception {
        this.initConnection(connectionFactory);

        // When
        connection.send(run("UNWIND [1,2,3] AS a RETURN a, a * a AS a_squared")).send(pull());

        // Then
        assertThat(connection)
                .receivesSuccess(meta -> assertThat(meta)
                        .containsKey("t_first")
                        .hasEntrySatisfying("fields", fields -> assertThat(fields)
                                .asInstanceOf(list(String.class))
                                .hasSize(2)
                                .containsExactly("a", "a_squared")))
                .receivesRecord(longValue(1), longValue(1))
                .receivesRecord(longValue(2), longValue(4))
                .receivesRecord(longValue(3), longValue(9))
                .receivesSuccess(meta -> assertThat(meta).containsKey("t_last").containsEntry("type", "r"));
    }

    @ParameterizedTest(name = "{displayName} {arguments}")
    @MethodSource("argumentsProvider")
    public void shouldRespondWithMetadataToDiscardAll(TransportConnection.Factory connectionFactory) throws Exception {
        this.initConnection(connectionFactory);

        // When
        connection.send(run("UNWIND [1,2,3] AS a RETURN a, a * a AS a_squared")).send(discard());

        // Then
        assertThat(connection)
                .receivesSuccess(meta -> assertThat(meta)
                        .containsKey("t_first")
                        .hasEntrySatisfying("fields", fields -> assertThat(fields)
                                .asInstanceOf(list(String.class))
                                .hasSize(2)
                                .containsExactly("a", "a_squared")))
                .receivesSuccess(meta -> assertThat(meta).containsKey("t_last").containsEntry("type", "r"));
    }

    @ParameterizedTest(name = "{displayName} {arguments}")
    @MethodSource("argumentsProvider")
    public void shouldBeAbleToRunQueryAfterAckFailure(TransportConnection.Factory connectionFactory) throws Exception {
        this.initConnection(connectionFactory);

        // Given
        connection.send(run("QINVALID")).send(pull());

        assertThat(connection)
                .receivesFailureFuzzy(Status.Statement.SyntaxError, "line 1, column 1")
                .receivesIgnored();

        // When
        connection.send(reset()).send(run("RETURN 1")).send(pull());

        // Then
        assertThat(connection)
                .receivesSuccess()
                .receivesSuccess()
                .receivesRecord(longValue(1))
                .receivesSuccess();
    }

    @ParameterizedTest(name = "{displayName} {arguments}")
    @MethodSource("argumentsProvider")
    public void shouldRunProcedure(TransportConnection.Factory connectionFactory) throws Exception {
        this.initConnection(connectionFactory);

        // Given
        connection.send(run("CREATE (n:Test {age: 2}) RETURN n.age AS age")).send(pull());

        assertThat(connection)
                .receivesSuccess(meta -> assertThat(meta)
                        .containsKey("t_first")
                        .hasEntrySatisfying("fields", fields -> assertThat(fields)
                                .asInstanceOf(list(String.class))
                                .hasSize(1)
                                .containsExactly("age")))
                .receivesRecord(longValue(2))
                .receivesSuccess(meta -> assertThat(meta).containsKey("t_last"));

        // When
        connection.send(run("CALL db.labels() YIELD label")).send(pull());

        // Then
        assertThat(connection)
                .receivesSuccess(meta -> assertThat(meta)
                        .containsKey("t_first")
                        .hasEntrySatisfying("fields", fields -> assertThat(fields)
                                .asInstanceOf(list(String.class))
                                .hasSize(1)
                                .containsExactly("label")))
                .receivesRecord(stringValue("Test"))
                .receivesSuccess();
    }

    @ParameterizedTest(name = "{displayName} {arguments}")
    @MethodSource("argumentsProvider")
    public void shouldHandleDeletedNodes(TransportConnection.Factory connectionFactory) throws Exception {
        this.initConnection(connectionFactory);

        // When
        connection.send(run("CREATE (n:Test) DELETE n RETURN n")).send(pull());

        // Then
        assertThat(connection)
                .receivesSuccess(meta -> assertThat(meta)
                        .containsKey("t_first")
                        .hasEntrySatisfying("fields", fields -> assertThat(fields)
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
                        .containsStruct(0x4E, 3)
                        .containsInt(0)
                        .containsList(labels -> assertThat(labels).isEmpty())
                        .containsMap(props -> assertThat(props).isEmpty())
                        .asBuffer()
                        .hasNoRemainingReadableBytes())
                .receivesSuccess(meta -> assertThat(meta).containsKey("t_last"));
    }

    @ParameterizedTest(name = "{displayName} {arguments}")
    @MethodSource("argumentsProvider")
    public void shouldHandleDeletedRelationships(TransportConnection.Factory connectionFactory) throws Exception {
        this.initConnection(connectionFactory);

        // When
        connection
                .send(run("CREATE (a)-[r:T {prop: 42}]->(b) DELETE r RETURN r"))
                .send(pull());

        // Then
        assertThat(connection)
                .receivesSuccess(meta -> assertThat(meta)
                        .containsKey("t_first")
                        .hasEntrySatisfying("fields", fields -> assertThat(fields)
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
                        .containsStruct(0x52, 5)
                        .containsInt(0)
                        .containsInt(-1)
                        .containsInt(-1)
                        .containsString("")
                        .containsMap(props -> assertThat(props).isEmpty())
                        .asBuffer()
                        .hasNoRemainingReadableBytes())
                .receivesSuccess();
    }

    @ParameterizedTest(name = "{displayName} {arguments}")
    @MethodSource("argumentsProvider")
    public void shouldNotLeakStatsToNextStatement(TransportConnection.Factory connectionFactory) throws Exception {
        this.initConnection(connectionFactory);

        // Given
        connection.send(run("CREATE (n)")).send(pull());

        assertThat(connection).receivesSuccess(2);

        // When
        connection.send(run("RETURN 1")).send(pull());

        // Then
        assertThat(connection).receivesSuccess().receivesRecord(longValue(1)).receivesSuccess(meta -> assertThat(meta)
                .containsKey("t_last")
                .containsEntry("type", "r"));
    }

    @ParameterizedTest(name = "{displayName} {arguments}")
    @MethodSource("argumentsProvider")
    public void shouldSendNotifications(TransportConnection.Factory connectionFactory) throws Exception {
        this.initConnection(connectionFactory);

        // When
        connection
                .send(run("EXPLAIN MATCH (a:THIS_IS_NOT_A_LABEL) RETURN count(*)"))
                .send(pull());

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

    @ParameterizedTest(name = "{displayName} {arguments}")
    @MethodSource("argumentsProvider")
    public void shouldFailNicelyWhenDroppingUnknownIndex(TransportConnection.Factory connectionFactory)
            throws Exception {
        this.initConnection(connectionFactory);

        // When
        connection.send(run("DROP INDEX my_index")).send(pull());

        // Then
        assertThat(connection)
                .receivesFailure(
                        Status.Schema.IndexDropFailed,
                        "Unable to drop index called `my_index`. There is no such index.")
                .receivesIgnored();
    }
}
