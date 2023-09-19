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
package org.neo4j.bolt.fsm;

import static org.assertj.core.api.Assertions.assertThat;
import static org.neo4j.bolt.testing.assertions.ResponseRecorderAssertions.assertThat;
import static org.neo4j.bolt.testing.assertions.StateMachineAssertions.assertThat;
import static org.neo4j.internal.helpers.Strings.joinAsLines;
import static org.neo4j.values.storable.Values.longValue;
import static org.neo4j.values.storable.Values.stringValue;

import java.io.IOException;
import java.io.PrintWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import org.neo4j.bolt.fsm.error.StateMachineException;
import org.neo4j.bolt.protocol.common.fsm.States;
import org.neo4j.bolt.protocol.common.fsm.response.NoopResponseHandler;
import org.neo4j.bolt.protocol.common.message.request.RequestMessage;
import org.neo4j.bolt.test.annotation.CommunityStateMachineTestExtension;
import org.neo4j.bolt.testing.annotation.fsm.StateMachineTest;
import org.neo4j.bolt.testing.annotation.fsm.initializer.Authenticated;
import org.neo4j.bolt.testing.annotation.fsm.initializer.Autocommit;
import org.neo4j.bolt.testing.assertions.ConnectionHandleAssertions;
import org.neo4j.bolt.testing.assertions.MapValueAssertions;
import org.neo4j.bolt.testing.messages.BoltMessages;
import org.neo4j.bolt.testing.response.ResponseRecorder;
import org.neo4j.internal.helpers.collection.MapUtil;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.kernel.impl.util.ValueUtils;
import org.neo4j.values.storable.LongValue;
import org.neo4j.values.virtual.MapValue;

@CommunityStateMachineTestExtension
public class AutocommitIT {

    @StateMachineTest
    void shouldExecuteStatement(@Authenticated StateMachine fsm, ResponseRecorder recorder, BoltMessages messages)
            throws Throwable {
        fsm.process(messages.run("CREATE (n {k:'k'}) RETURN n.k"), recorder);

        assertThat(fsm).isInState(States.AUTO_COMMIT);

        fsm.process(messages.pull(), recorder);

        assertThat(recorder)
                .hasSuccessResponse(meta -> MapValueAssertions.assertThat(meta)
                        .containsKey("fields")
                        .containsKey("t_first"))
                .hasRecord(stringValue("k"))
                .hasSuccessResponse();

        assertThat(fsm).isInState(States.READY);
    }

    @StateMachineTest
    void shouldHandleImplicitCommitFailure(
            @Authenticated StateMachine fsm, ResponseRecorder recorder, BoltMessages messages) throws Throwable {
        fsm.process(messages.run("CREATE (n:Victim)-[:REL]->()"), NoopResponseHandler.getInstance());
        fsm.process(messages.discard(), NoopResponseHandler.getInstance());

        fsm.process(messages.run("MATCH (n:Victim) DELETE n"), recorder);
        fsm.process(messages.discard(), recorder);

        assertThat(recorder).hasSuccessResponse().hasFailureResponse(Status.Schema.ConstraintValidationFailed);
    }

    @StateMachineTest
    void shouldBeAbleToCleanlyRunMultipleSessionsInSingleThread(
            @Authenticated StateMachine fsm1,
            @Authenticated StateMachine fsm2,
            BoltMessages messages,
            ResponseRecorder recorder)
            throws Throwable {

        // And given I've started a transaction in one session
        fsm1.process(messages.begin(), recorder);

        assertThat(recorder).hasSuccessResponse();

        // When I issue a statement in a separate session
        fsm2.process(messages.run("CREATE (a:Person) RETURN id(a)"), recorder);
        fsm2.process(messages.pull(), recorder);

        assertThat(recorder).hasSuccessResponse();

        var response = recorder.next();
        assertThat(response.isRecord()).isTrue();

        var id = ((LongValue) response.asRecord()[0]).longValue();

        assertThat(recorder).hasSuccessResponse();

        // And when I roll back that first session transaction
        fsm1.process(messages.rollback(), recorder);

        assertThat(recorder).hasSuccessResponse();

        // Then the two should not have interfered with each other
        recorder.reset();
        fsm2.process(messages.run("MATCH (a:Person) WHERE id(a) = " + id + " RETURN COUNT(*)"), recorder);
        fsm2.process(messages.pull(), recorder);

        assertThat(recorder).hasSuccessResponse().hasRecord(longValue(1));
    }

    @StateMachineTest
    void shouldSupportUsingExplainCallInTransactionsInTransaction(
            @Authenticated StateMachine fsm, BoltMessages messages, ResponseRecorder recorder) throws Exception {
        // Given
        var params = map("csvFileUrl", createLocalIrisData(fsm, messages));

        // When
        fsm.process(messages.begin(), NoopResponseHandler.getInstance());
        fsm.process(
                messages.run(
                        joinAsLines(
                                "LOAD CSV WITH HEADERS FROM $csvFileUrl AS l",
                                "CALL {",
                                "  WITH l",
                                "  MATCH (c:Class {name: l.class_name})",
                                "  CREATE (s:Sample {sepal_length: l.sepal_length,",
                                "                    sepal_width: l.sepal_width,",
                                "                    petal_length: l.petal_length,",
                                "                    petal_width: l.petal_width})",
                                "  CREATE (c)<-[:HAS_CLASS]-(s)",
                                "  RETURN c, s",
                                "} IN TRANSACTIONS OF 40 ROWS",
                                "RETURN count(*) AS c"),
                        params),
                recorder);

        // Then
        assertThat(recorder).hasSuccessResponse();
    }

    @StateMachineTest
    void shouldCloseTransactionOnCommit(@Authenticated StateMachine fsm, BoltMessages messages) throws Exception {
        fsm.process(messages.begin(), NoopResponseHandler.getInstance());
        runAndPull(fsm, messages);
        fsm.process(messages.commit(), NoopResponseHandler.getInstance());

        ConnectionHandleAssertions.assertThat(fsm.connection()).hasNoTransaction();
    }

    @StateMachineTest
    void shouldCloseTransactionOnRollback(@Authenticated StateMachine fsm, BoltMessages messages) throws Exception {
        fsm.process(messages.begin(), NoopResponseHandler.getInstance());
        runAndPull(fsm, messages);
        fsm.process(messages.rollback(), NoopResponseHandler.getInstance());

        ConnectionHandleAssertions.assertThat(fsm.connection()).hasNoTransaction();
    }

    private void shouldTerminateConnectionOnMessage(StateMachine fsm, RequestMessage message) {
        var recorder = new ResponseRecorder();

        assertThat(fsm).shouldKillConnection(it -> it.process(message, recorder));

        assertThat(recorder).hasFailureResponse(Status.Request.Invalid);
    }

    @StateMachineTest
    void shouldTerminateConnectionOnHello(@Autocommit StateMachine fsm, BoltMessages messages) {
        shouldTerminateConnectionOnMessage(fsm, messages.hello());
    }

    @StateMachineTest
    void shouldTerminateConnectionOnBegin(@Autocommit StateMachine fsm, BoltMessages messages) {
        shouldTerminateConnectionOnMessage(fsm, messages.begin());
    }

    @StateMachineTest
    void shouldTerminateConnectionOnRun(@Autocommit StateMachine fsm, BoltMessages messages) {
        shouldTerminateConnectionOnMessage(fsm, messages.run());
    }

    @StateMachineTest
    void shouldTerminateConnectionOnCommit(@Autocommit StateMachine fsm, BoltMessages messages) {
        shouldTerminateConnectionOnMessage(fsm, messages.commit());
    }

    @StateMachineTest
    void shouldTerminateConnectionOnRollback(@Autocommit StateMachine fsm, BoltMessages messages) {
        shouldTerminateConnectionOnMessage(fsm, messages.rollback());
    }

    @StateMachineTest
    void shouldTerminateConnectionOnReset(@Autocommit StateMachine fsm, BoltMessages messages) {
        shouldTerminateConnectionOnMessage(fsm, messages.reset());
    }

    @StateMachineTest
    void shouldTerminateConnectionOnGoodbye(@Autocommit StateMachine fsm, BoltMessages messages) {
        shouldTerminateConnectionOnMessage(fsm, messages.goodbye());
    }

    static String createLocalIrisData(StateMachine machine, BoltMessages messages)
            throws IOException, StateMachineException {
        for (String className : IRIS_CLASS_NAMES) {
            MapValue params = map("className", className);
            runAndPull(machine, messages, "CREATE (c:Class {name: $className}) RETURN c", params);
        }

        Path tempFile = Files.createTempFile("iris", ".csv");
        tempFile.toFile().deleteOnExit();
        try (PrintWriter out = new PrintWriter(Files.newOutputStream(tempFile), false, StandardCharsets.UTF_8)) {
            out.println(IRIS_DATA);
        }
        return tempFile.toUri().toURL().toExternalForm();
    }

    private static void runAndPull(StateMachine machine, BoltMessages messages) throws StateMachineException {
        runAndPull(machine, messages, "RETURN 1", MapValue.EMPTY);
    }

    private static void runAndPull(StateMachine fsm, BoltMessages messages, String statement, MapValue params)
            throws StateMachineException {
        var recorder = new ResponseRecorder();

        fsm.process(messages.run(statement, params), recorder);
        fsm.process(messages.pull(), recorder);

        assertThat(recorder).hasSuccessResponse().hasRecord().hasSuccessResponse();
    }

    private static MapValue map(Object... keyValues) {
        return ValueUtils.asMapValue(MapUtil.map(keyValues));
    }

    private static final String[] IRIS_CLASS_NAMES = new String[] {"Iris-setosa", "Iris-versicolor", "Iris-virginica"};

    static final String IRIS_DATA =
            "sepal_length,sepal_width,petal_length,petal_width,class_name\n" + "5.1,3.5,1.4,0.2,Iris-setosa\n"
                    + "4.9,3.0,1.4,0.2,Iris-setosa\n"
                    + "4.7,3.2,1.3,0.2,Iris-setosa\n"
                    + "4.6,3.1,1.5,0.2,Iris-setosa\n"
                    + "5.0,3.6,1.4,0.2,Iris-setosa\n"
                    + "5.4,3.9,1.7,0.4,Iris-setosa\n"
                    + "4.6,3.4,1.4,0.3,Iris-setosa\n"
                    + "5.0,3.4,1.5,0.2,Iris-setosa\n"
                    + "4.4,2.9,1.4,0.2,Iris-setosa\n"
                    + "4.9,3.1,1.5,0.1,Iris-setosa\n"
                    + "5.4,3.7,1.5,0.2,Iris-setosa\n"
                    + "4.8,3.4,1.6,0.2,Iris-setosa\n"
                    + "4.8,3.0,1.4,0.1,Iris-setosa\n"
                    + "4.3,3.0,1.1,0.1,Iris-setosa\n"
                    + "5.8,4.0,1.2,0.2,Iris-setosa\n"
                    + "5.7,4.4,1.5,0.4,Iris-setosa\n"
                    + "5.4,3.9,1.3,0.4,Iris-setosa\n"
                    + "5.1,3.5,1.4,0.3,Iris-setosa\n"
                    + "5.7,3.8,1.7,0.3,Iris-setosa\n"
                    + "5.1,3.8,1.5,0.3,Iris-setosa\n"
                    + "5.4,3.4,1.7,0.2,Iris-setosa\n"
                    + "5.1,3.7,1.5,0.4,Iris-setosa\n"
                    + "4.6,3.6,1.0,0.2,Iris-setosa\n"
                    + "5.1,3.3,1.7,0.5,Iris-setosa\n"
                    + "4.8,3.4,1.9,0.2,Iris-setosa\n"
                    + "5.0,3.0,1.6,0.2,Iris-setosa\n"
                    + "5.0,3.4,1.6,0.4,Iris-setosa\n"
                    + "5.2,3.5,1.5,0.2,Iris-setosa\n"
                    + "5.2,3.4,1.4,0.2,Iris-setosa\n"
                    + "4.7,3.2,1.6,0.2,Iris-setosa\n"
                    + "4.8,3.1,1.6,0.2,Iris-setosa\n"
                    + "5.4,3.4,1.5,0.4,Iris-setosa\n"
                    + "5.2,4.1,1.5,0.1,Iris-setosa\n"
                    + "5.5,4.2,1.4,0.2,Iris-setosa\n"
                    + "4.9,3.1,1.5,0.2,Iris-setosa\n"
                    + "5.0,3.2,1.2,0.2,Iris-setosa\n"
                    + "5.5,3.5,1.3,0.2,Iris-setosa\n"
                    + "4.9,3.6,1.4,0.1,Iris-setosa\n"
                    + "4.4,3.0,1.3,0.2,Iris-setosa\n"
                    + "5.1,3.4,1.5,0.2,Iris-setosa\n"
                    + "5.0,3.5,1.3,0.3,Iris-setosa\n"
                    + "4.5,2.3,1.3,0.3,Iris-setosa\n"
                    + "4.4,3.2,1.3,0.2,Iris-setosa\n"
                    + "5.0,3.5,1.6,0.6,Iris-setosa\n"
                    + "5.1,3.8,1.9,0.4,Iris-setosa\n"
                    + "4.8,3.0,1.4,0.3,Iris-setosa\n"
                    + "5.1,3.8,1.6,0.2,Iris-setosa\n"
                    + "4.6,3.2,1.4,0.2,Iris-setosa\n"
                    + "5.3,3.7,1.5,0.2,Iris-setosa\n"
                    + "5.0,3.3,1.4,0.2,Iris-setosa\n"
                    + "7.0,3.2,4.7,1.4,Iris-versicolor\n"
                    + "6.4,3.2,4.5,1.5,Iris-versicolor\n"
                    + "6.9,3.1,4.9,1.5,Iris-versicolor\n"
                    + "5.5,2.3,4.0,1.3,Iris-versicolor\n"
                    + "6.5,2.8,4.6,1.5,Iris-versicolor\n"
                    + "5.7,2.8,4.5,1.3,Iris-versicolor\n"
                    + "6.3,3.3,4.7,1.6,Iris-versicolor\n"
                    + "4.9,2.4,3.3,1.0,Iris-versicolor\n"
                    + "6.6,2.9,4.6,1.3,Iris-versicolor\n"
                    + "5.2,2.7,3.9,1.4,Iris-versicolor\n"
                    + "5.0,2.0,3.5,1.0,Iris-versicolor\n"
                    + "5.9,3.0,4.2,1.5,Iris-versicolor\n"
                    + "6.0,2.2,4.0,1.0,Iris-versicolor\n"
                    + "6.1,2.9,4.7,1.4,Iris-versicolor\n"
                    + "5.6,2.9,3.6,1.3,Iris-versicolor\n"
                    + "6.7,3.1,4.4,1.4,Iris-versicolor\n"
                    + "5.6,3.0,4.5,1.5,Iris-versicolor\n"
                    + "5.8,2.7,4.1,1.0,Iris-versicolor\n"
                    + "6.2,2.2,4.5,1.5,Iris-versicolor\n"
                    + "5.6,2.5,3.9,1.1,Iris-versicolor\n"
                    + "5.9,3.2,4.8,1.8,Iris-versicolor\n"
                    + "6.1,2.8,4.0,1.3,Iris-versicolor\n"
                    + "6.3,2.5,4.9,1.5,Iris-versicolor\n"
                    + "6.1,2.8,4.7,1.2,Iris-versicolor\n"
                    + "6.4,2.9,4.3,1.3,Iris-versicolor\n"
                    + "6.6,3.0,4.4,1.4,Iris-versicolor\n"
                    + "6.8,2.8,4.8,1.4,Iris-versicolor\n"
                    + "6.7,3.0,5.0,1.7,Iris-versicolor\n"
                    + "6.0,2.9,4.5,1.5,Iris-versicolor\n"
                    + "5.7,2.6,3.5,1.0,Iris-versicolor\n"
                    + "5.5,2.4,3.8,1.1,Iris-versicolor\n"
                    + "5.5,2.4,3.7,1.0,Iris-versicolor\n"
                    + "5.8,2.7,3.9,1.2,Iris-versicolor\n"
                    + "6.0,2.7,5.1,1.6,Iris-versicolor\n"
                    + "5.4,3.0,4.5,1.5,Iris-versicolor\n"
                    + "6.0,3.4,4.5,1.6,Iris-versicolor\n"
                    + "6.7,3.1,4.7,1.5,Iris-versicolor\n"
                    + "6.3,2.3,4.4,1.3,Iris-versicolor\n"
                    + "5.6,3.0,4.1,1.3,Iris-versicolor\n"
                    + "5.5,2.5,4.0,1.3,Iris-versicolor\n"
                    + "5.5,2.6,4.4,1.2,Iris-versicolor\n"
                    + "6.1,3.0,4.6,1.4,Iris-versicolor\n"
                    + "5.8,2.6,4.0,1.2,Iris-versicolor\n"
                    + "5.0,2.3,3.3,1.0,Iris-versicolor\n"
                    + "5.6,2.7,4.2,1.3,Iris-versicolor\n"
                    + "5.7,3.0,4.2,1.2,Iris-versicolor\n"
                    + "5.7,2.9,4.2,1.3,Iris-versicolor\n"
                    + "6.2,2.9,4.3,1.3,Iris-versicolor\n"
                    + "5.1,2.5,3.0,1.1,Iris-versicolor\n"
                    + "5.7,2.8,4.1,1.3,Iris-versicolor\n"
                    + "6.3,3.3,6.0,2.5,Iris-virginica\n"
                    + "5.8,2.7,5.1,1.9,Iris-virginica\n"
                    + "7.1,3.0,5.9,2.1,Iris-virginica\n"
                    + "6.3,2.9,5.6,1.8,Iris-virginica\n"
                    + "6.5,3.0,5.8,2.2,Iris-virginica\n"
                    + "7.6,3.0,6.6,2.1,Iris-virginica\n"
                    + "4.9,2.5,4.5,1.7,Iris-virginica\n"
                    + "7.3,2.9,6.3,1.8,Iris-virginica\n"
                    + "6.7,2.5,5.8,1.8,Iris-virginica\n"
                    + "7.2,3.6,6.1,2.5,Iris-virginica\n"
                    + "6.5,3.2,5.1,2.0,Iris-virginica\n"
                    + "6.4,2.7,5.3,1.9,Iris-virginica\n"
                    + "6.8,3.0,5.5,2.1,Iris-virginica\n"
                    + "5.7,2.5,5.0,2.0,Iris-virginica\n"
                    + "5.8,2.8,5.1,2.4,Iris-virginica\n"
                    + "6.4,3.2,5.3,2.3,Iris-virginica\n"
                    + "6.5,3.0,5.5,1.8,Iris-virginica\n"
                    + "7.7,3.8,6.7,2.2,Iris-virginica\n"
                    + "7.7,2.6,6.9,2.3,Iris-virginica\n"
                    + "6.0,2.2,5.0,1.5,Iris-virginica\n"
                    + "6.9,3.2,5.7,2.3,Iris-virginica\n"
                    + "5.6,2.8,4.9,2.0,Iris-virginica\n"
                    + "7.7,2.8,6.7,2.0,Iris-virginica\n"
                    + "6.3,2.7,4.9,1.8,Iris-virginica\n"
                    + "6.7,3.3,5.7,2.1,Iris-virginica\n"
                    + "7.2,3.2,6.0,1.8,Iris-virginica\n"
                    + "6.2,2.8,4.8,1.8,Iris-virginica\n"
                    + "6.1,3.0,4.9,1.8,Iris-virginica\n"
                    + "6.4,2.8,5.6,2.1,Iris-virginica\n"
                    + "7.2,3.0,5.8,1.6,Iris-virginica\n"
                    + "7.4,2.8,6.1,1.9,Iris-virginica\n"
                    + "7.9,3.8,6.4,2.0,Iris-virginica\n"
                    + "6.4,2.8,5.6,2.2,Iris-virginica\n"
                    + "6.3,2.8,5.1,1.5,Iris-virginica\n"
                    + "6.1,2.6,5.6,1.4,Iris-virginica\n"
                    + "7.7,3.0,6.1,2.3,Iris-virginica\n"
                    + "6.3,3.4,5.6,2.4,Iris-virginica\n"
                    + "6.4,3.1,5.5,1.8,Iris-virginica\n"
                    + "6.0,3.0,4.8,1.8,Iris-virginica\n"
                    + "6.9,3.1,5.4,2.1,Iris-virginica\n"
                    + "6.7,3.1,5.6,2.4,Iris-virginica\n"
                    + "6.9,3.1,5.1,2.3,Iris-virginica\n"
                    + "5.8,2.7,5.1,1.9,Iris-virginica\n"
                    + "6.8,3.2,5.9,2.3,Iris-virginica\n"
                    + "6.7,3.3,5.7,2.5,Iris-virginica\n"
                    + "6.7,3.0,5.2,2.3,Iris-virginica\n"
                    + "6.3,2.5,5.0,1.9,Iris-virginica\n"
                    + "6.5,3.0,5.2,2.0,Iris-virginica\n"
                    + "6.2,3.4,5.4,2.3,Iris-virginica\n"
                    + "5.9,3.0,5.1,1.8,Iris-virginica\n"
                    + "\n";
}
