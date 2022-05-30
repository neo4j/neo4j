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
package org.neo4j.bolt.v4.runtime;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.bolt.testing.NullResponseHandler.nullResponseHandler;
import static org.neo4j.bolt.testing.assertions.ResponseRecorderAssertions.assertThat;
import static org.neo4j.bolt.testing.assertions.StateMachineAssertions.assertThat;
import static org.neo4j.bolt.testing.messages.BoltV40Messages.begin;
import static org.neo4j.bolt.testing.messages.BoltV40Messages.commit;
import static org.neo4j.bolt.testing.messages.BoltV40Messages.discard;
import static org.neo4j.bolt.testing.messages.BoltV40Messages.pull;
import static org.neo4j.bolt.testing.messages.BoltV40Messages.reset;
import static org.neo4j.bolt.testing.messages.BoltV40Messages.rollback;
import static org.neo4j.bolt.testing.messages.BoltV40Messages.run;
import static org.neo4j.internal.helpers.Strings.joinAsLines;
import static org.neo4j.values.storable.Values.longValue;
import static org.neo4j.values.storable.Values.stringValue;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;
import org.neo4j.bolt.protocol.common.fsm.AbstractStateMachine;
import org.neo4j.bolt.protocol.common.fsm.StateMachine;
import org.neo4j.bolt.protocol.common.message.Error;
import org.neo4j.bolt.protocol.common.message.result.BoltResult;
import org.neo4j.bolt.protocol.common.message.result.ResponseHandler;
import org.neo4j.bolt.protocol.v40.fsm.StateMachineV40;
import org.neo4j.bolt.runtime.BoltConnectionFatality;
import org.neo4j.bolt.runtime.SessionExtension;
import org.neo4j.bolt.testing.response.ResponseRecorder;
import org.neo4j.bolt.transaction.StatementProcessorTxManager;
import org.neo4j.bolt.transaction.TransactionManager;
import org.neo4j.internal.helpers.collection.MapUtil;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.kernel.impl.util.ValueUtils;
import org.neo4j.values.AnyValue;
import org.neo4j.values.storable.LongValue;
import org.neo4j.values.virtual.MapValue;

class BoltConnectionIT extends BoltStateMachineV4StateTestBase {
    @Test
    void shouldExecuteStatement() throws Throwable {
        // Given
        var runRecorder = new ResponseRecorder();
        var machine = newStateMachineAfterAuth();

        // When
        machine.process(run("CREATE (n {k:'k'}) RETURN n.k"), runRecorder);

        // Then
        assertThat(runRecorder).hasSuccessResponse();

        // Given
        var pullRecorder = new ResponseRecorder();

        // When
        machine.process(pull(), pullRecorder);

        // Then
        assertThat(pullRecorder).hasRecord(stringValue("k"));
    }

    @Test
    void shouldHandleImplicitCommitFailure() throws Throwable {
        // Given
        var machine = newStateMachineAfterAuth();
        var recorder = new ResponseRecorder();

        machine.process(run("CREATE (n:Victim)-[:REL]->()"), nullResponseHandler());
        machine.process(discard(), nullResponseHandler());

        // When I perform an action that will fail on commit
        machine.process(run("MATCH (n:Victim) DELETE n"), recorder);

        // Then the statement running should have succeeded
        assertThat(recorder).hasSuccessResponse();

        // But the streaming should have failed, since it implicitly triggers commit and thus triggers a failure
        machine.process(discard(), recorder);

        assertThat(recorder).hasFailureResponse(Status.Schema.ConstraintValidationFailed);
    }

    @Test
    void shouldAllowUserControlledRollbackOnExplicitTxFailure() throws Throwable {
        // Given whenever en explicit transaction has a failure,
        // it is more natural for drivers to see the failure, acknowledge it
        // and send a `RESET`, because that means that all failures in the
        // transaction, be they client-local or inside neo, can be handled the
        // same way by a driver.
        var recorder = new ResponseRecorder();
        var machine = newStateMachineAfterAuth();

        machine.process(begin(), nullResponseHandler());
        machine.process(run("CREATE (n:Victim)-[:REL]->()"), nullResponseHandler());
        machine.process(discard(), nullResponseHandler());

        // When I perform an action that will fail
        machine.process(run("this is not valid syntax"), recorder);

        // Then I should see a failure
        assertThat(recorder).hasFailureResponse(Status.Statement.SyntaxError);

        // This result in an illegal state change, and closes all open statement by default.
        assertThat(machine).doesNotHaveOpenStatement();

        // And when I reset that failure, and the tx should be rolled back
        recorder.reset();
        resetReceived(machine, recorder);

        // Then both operations should succeed
        assertThat(recorder).hasSuccessResponse();
        assertThat(machine).doesNotHaveOpenStatement();
    }

    @Test
    void shouldHandleFailureDuringResultPublishing() throws Throwable {
        // Given
        var machine = newStateMachineAfterAuth();
        var pullAllCallbackCalled = new CountDownLatch(1);
        var error = new AtomicReference<Error>();

        // When something fails while publishing the result stream
        machine.process(run(), nullResponseHandler());
        machine.process(pull(), new ResponseHandler() {
            @Override
            public boolean onPullRecords(BoltResult result, long size) {
                throw new RuntimeException("Ooopsies!");
            }

            @Override
            public boolean onDiscardRecords(BoltResult result, long size) {
                throw new RuntimeException("Not this one!");
            }

            @Override
            public void onMetadata(String key, AnyValue value) {}

            @Override
            public void markFailed(Error err) {
                error.set(err);
                pullAllCallbackCalled.countDown();
            }

            @Override
            public void markIgnored() {}

            @Override
            public void onFinish() {}
        });

        // Then
        assertTrue(pullAllCallbackCalled.await(30, TimeUnit.SECONDS));
        var err = error.get();
        assertThat(err.status()).isEqualTo(Status.General.UnknownError);
        assertThat(err.message()).contains("Ooopsies!");
    }

    @Test
    void shouldBeAbleToCleanlyRunMultipleSessionsInSingleThread() throws Throwable {
        // Given
        var recorder = new ResponseRecorder();
        var firstMachine = newStateMachineAfterAuth("conn1");
        var secondMachine = newStateMachineAfterAuth("conn2");

        // And given I've started a transaction in one session
        firstMachine.process(begin(), recorder);

        assertThat(recorder).hasSuccessResponse();

        // When I issue a statement in a separate session
        secondMachine.process(run("CREATE (a:Person) RETURN id(a)"), recorder);
        secondMachine.process(pull(), recorder);

        assertThat(recorder).hasSuccessResponse();

        var response = recorder.next();
        assertThat(response.isRecord()).isTrue();

        var id = ((LongValue) response.asRecord()[0]).longValue();

        assertThat(recorder).hasSuccessResponse();

        // And when I roll back that first session transaction
        firstMachine.process(rollback(), recorder);

        assertThat(recorder).hasSuccessResponse();

        // Then the two should not have interfered with each other
        recorder.reset();
        secondMachine.process(run("MATCH (a:Person) WHERE id(a) = " + id + " RETURN COUNT(*)"), recorder);
        secondMachine.process(pull(), recorder);

        assertThat(recorder).hasSuccessResponse().hasRecord(longValue(1));
    }

    @Test
    void shouldSupportUsingExplainCallInTransactionsInTransaction() throws Exception {
        // Given
        ResponseRecorder recorder = new ResponseRecorder();
        var machine = newStateMachineAfterAuth();
        var params = map("csvFileUrl", createLocalIrisData(machine));

        // When
        machine.process(begin(), nullResponseHandler());
        machine.process(
                run(
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

    @Test
    void shouldCloseTransactionOnCommit() throws Exception {
        // Given
        var machine = newStateMachineAfterAuth();

        machine.process(begin(), nullResponseHandler());
        runAndPull(machine);
        machine.process(commit(), nullResponseHandler());

        assertThat(machine).doesNotHaveTransaction();
    }

    @Test
    void shouldCloseTransactionEvenIfCommitFails() throws Exception {
        // Given
        var recorder = new ResponseRecorder();
        var machine = newStateMachineAfterAuth();

        machine.process(begin(), nullResponseHandler());
        machine.process(run("X"), recorder);
        machine.process(pull(), recorder);

        assertThat(recorder).hasFailureResponse(Status.Statement.SyntaxError).hasIgnoredResponse();

        // The tx shall still be open.
        assertThat(machine).hasTransaction();

        recorder.reset();
        machine.process(commit(), recorder);

        assertThat(recorder).hasIgnoredResponse();

        resetReceived(machine, recorder);

        assertThat(recorder).hasSuccessResponse();

        assertThat(machine).doesNotHaveTransaction();
    }

    @Test
    void shouldCloseTransactionOnRollback() throws Exception {
        // Given
        var machine = newStateMachineAfterAuth();

        machine.process(begin(), nullResponseHandler());
        runAndPull(machine);
        machine.process(rollback(), nullResponseHandler());

        assertThat(machine).doesNotHaveTransaction();
    }

    @Test
    void shouldCloseTransactionOnRollbackAfterFailure() throws Exception {
        // Given
        var recorder = new ResponseRecorder();
        var machine = newStateMachineAfterAuth();

        machine.process(begin(), nullResponseHandler());
        machine.process(run("X"), recorder);
        machine.process(pull(), recorder);

        assertThat(recorder).hasFailureResponse(Status.Statement.SyntaxError).hasIgnoredResponse();

        // The tx shall still be open.
        assertThat(machine).hasTransaction();

        recorder.reset();
        machine.process(rollback(), recorder);

        assertThat(recorder).hasIgnoredResponse();

        resetReceived(machine, recorder);

        assertThat(recorder).hasSuccessResponse();

        assertThat(machine).doesNotHaveTransaction();
    }

    private static void resetReceived(StateMachineV40 machine, ResponseRecorder recorder)
            throws BoltConnectionFatality {
        // Reset is two steps now: When parsing reset message we immediately interrupt, and then ignores all other
        // messages until reset is reached.
        machine.interrupt();
        machine.process(reset(), recorder);
    }

    private static boolean hasTransaction(StateMachine machine) {
        TransactionManager txManager =
                ((AbstractStateMachine) machine).stateMachineContext().transactionManager();
        return ((StatementProcessorTxManager) txManager).getCurrentNoOfOpenTx() > 0;
    }

    static String createLocalIrisData(StateMachine machine) throws Exception {
        for (String className : IRIS_CLASS_NAMES) {
            MapValue params = map("className", className);
            runAndPull(machine, "CREATE (c:Class {name: $className}) RETURN c", params);
        }

        return SessionExtension.putTmpFile("iris", ".csv", IRIS_DATA).toExternalForm();
    }

    private static void runAndPull(StateMachine machine) throws Exception {
        runAndPull(machine, "RETURN 1", EMPTY_PARAMS);
    }

    private static void runAndPull(StateMachine machine, String statement, MapValue params) throws Exception {
        var recorder = new ResponseRecorder();

        machine.process(run(statement, params), recorder);
        machine.process(pull(), recorder);

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
