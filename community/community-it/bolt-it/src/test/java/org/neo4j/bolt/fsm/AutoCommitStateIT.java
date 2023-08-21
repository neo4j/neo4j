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

import static org.neo4j.bolt.testing.assertions.MapValueAssertions.assertThat;
import static org.neo4j.bolt.testing.assertions.ResponseRecorderAssertions.assertThat;
import static org.neo4j.bolt.testing.assertions.StateMachineAssertions.assertThat;
import static org.neo4j.values.storable.BooleanValue.TRUE;
import static org.neo4j.values.storable.Values.longValue;

import org.neo4j.bolt.fsm.error.StateMachineException;
import org.neo4j.bolt.protocol.common.fsm.States;
import org.neo4j.bolt.protocol.common.message.request.RequestMessage;
import org.neo4j.bolt.test.annotation.CommunityStateMachineTestExtension;
import org.neo4j.bolt.testing.annotation.fsm.StateMachineTest;
import org.neo4j.bolt.testing.annotation.fsm.initializer.Autocommit;
import org.neo4j.bolt.testing.messages.BoltMessages;
import org.neo4j.bolt.testing.response.ResponseRecorder;
import org.neo4j.kernel.api.exceptions.Status;

@CommunityStateMachineTestExtension
class AutoCommitStateIT {

    /**
     * Evaluates whether the state machine moves from AUTOCOMMIT back to READY once all messages are
     * consumed (single record variation).
     */
    @StateMachineTest
    void shouldMoveFromAutoCommitToReadyOnPullWhenSingleResultIsReturned(
            @Autocommit StateMachine fsm, ResponseRecorder recorder, BoltMessages messages) throws Throwable {
        fsm.process(messages.pull(1), recorder);

        assertThat(recorder).hasRecord().hasSuccessResponse(meta -> assertThat(meta)
                .containsKey("type")
                .containsKey("t_last")
                .containsKey("bookmark")
                .containsKey("db"));

        assertThat(fsm).isInState(States.READY);
    }

    /**
     * Evaluates whether the state machine moves from AUTOCOMMIT back to READY once all messages are
     * consumed (multi record variation).
     */
    @StateMachineTest
    void shouldMoveFromAutoCommitToReadyOnPullWhenMultiplyResultsAreReturned(
            @Autocommit("UNWIND [1, 2, 3] AS n RETURN n") StateMachine fsm,
            ResponseRecorder recorder,
            BoltMessages messages)
            throws Throwable {
        fsm.process(messages.pull(2), recorder);

        assertThat(recorder).hasRecord(longValue(1)).hasRecord(longValue(2)).hasSuccessResponse(meta -> assertThat(meta)
                .containsEntry("has_more", TRUE)
                .doesNotContainKey("db")
                .doesNotContainKey("bookmark"));

        fsm.process(messages.pull(2), recorder);

        assertThat(recorder).hasRecord(longValue(3)).hasSuccessResponse(meta -> assertThat(meta)
                .containsKey("type")
                .containsKey("t_last")
                .containsKey("bookmark")
                .containsKey("db"));

        assertThat(fsm).isInState(States.READY);
    }

    /**
     * Evaluates whether the state machine moves from AUTOCOMMIT back to READY once all messages are
     * discarded (single record variation).
     */
    @StateMachineTest
    void shouldMoveFromAutoCommitToReadyOnDiscardAllWhenSingleResultIsReturned(
            @Autocommit StateMachine fsm, ResponseRecorder recorder, BoltMessages messages) throws Throwable {
        fsm.process(messages.discard(1), recorder);

        assertThat(recorder)
                .hasSuccessResponse(
                        meta -> assertThat(meta).containsKey("bookmark").containsKey("db"));

        assertThat(fsm).isInState(States.READY);
    }

    /**
     * Evaluates whether the state machine moves from AUTOCOMMIT back to READY once all messages are
     * discarded (multi record variation).
     */
    @StateMachineTest
    void shouldMoveFromAutoCommitToReadyOnDiscardAllWhenMultipleResultsAreReturned(
            @Autocommit("UNWIND [1, 2, 3] AS n RETURN n") StateMachine fsm,
            ResponseRecorder recorder,
            BoltMessages messages)
            throws Throwable {
        fsm.process(messages.discard(2), recorder);

        assertThat(recorder).hasSuccessResponse(meta -> assertThat(meta)
                .containsEntry("has_more", TRUE)
                .doesNotContainKey("db")
                .doesNotContainKey("bookmark"));

        fsm.process(messages.discard(2), recorder);

        assertThat(recorder).hasSuccessResponse(meta -> assertThat(meta)
                .containsKey("type")
                .containsKey("t_last")
                .containsKey("bookmark")
                .containsKey("db"));

        assertThat(fsm).isInState(States.READY);
    }

    /**
     * Evaluates whether the state machine moves to the INTERRUPTED state when an INTERRUPT signal
     * is received.
     * <p />
     * Note that the INTERRUPT signal is an implicit result of receiving a RESET message but is
     * distinct from RESET in order to determine the exact moment at which the machine shall return
     * to READY.
     */
    @StateMachineTest
    void shouldMoveFromAutoCommitToInterruptedOnInterrupt(
            @Autocommit StateMachine fsm, BoltMessages messages, ResponseRecorder recorder)
            throws StateMachineException {
        fsm.connection().interrupt();

        fsm.process(messages.run("RETURN 1"), recorder);

        assertThat(recorder).hasIgnoredResponse();

        assertThat(fsm).isInterrupted();
    }

    /**
     * Evaluates whether the connection is terminated when HELLO is received while in auto commit
     * state.
     * <p />
     * HELLO is only valid within CONNECTED state.
     */
    @StateMachineTest
    void shouldCloseConnectionInAutoCommitOnHello(
            @Autocommit StateMachine fsm, ResponseRecorder recorder, BoltMessages messages) {
        this.shouldCloseConnectionInAutoCommitOnMessage(fsm, recorder, messages.hello());
    }

    /**
     * Evaluates whether the connection is terminated when RUN is received while in auto commit
     * state.
     * <p />
     * RUN is only valid within READY and TX_READY state.
     */
    @StateMachineTest
    void shouldCloseConnectionInAutoCommitOnRun(
            @Autocommit StateMachine fsm, ResponseRecorder recorder, BoltMessages messages) {
        // explicitly send a valid cypher query so that we do not end up with another failure
        // message when this test is supposed to fail
        this.shouldCloseConnectionInAutoCommitOnMessage(fsm, recorder, messages.run("RETURN 1"));
    }

    /**
     * Evaluates whether the connection is terminated when BEGIN is received while in auto commit
     * state.
     * <p />
     * BEGIN is only valid within READY state.
     */
    @StateMachineTest
    void shouldCloseConnectionInAutoCommitOnBegin(
            @Autocommit StateMachine fsm, ResponseRecorder recorder, BoltMessages messages) {
        this.shouldCloseConnectionInAutoCommitOnMessage(fsm, recorder, messages.begin());
    }

    /**
     * Evaluates whether the connection is terminated when COMMIT is received while in auto commit
     * state.
     * <p />
     * COMMIT is only valid within TX_READY state.
     */
    @StateMachineTest
    void shouldCloseConnectionInAutoCommitOnCommit(
            @Autocommit StateMachine fsm, ResponseRecorder recorder, BoltMessages messages) {
        this.shouldCloseConnectionInAutoCommitOnMessage(fsm, recorder, messages.commit());
    }

    /**
     * Evaluates whether the connection is terminated when ROLLBACK is received while in auto commit
     * state.
     * <p />
     * ROLLBACK is only valid within TX_READY state.
     */
    @StateMachineTest
    void shouldCloseConnectionInAutoCommitOnRollback(
            @Autocommit StateMachine fsm, ResponseRecorder recorder, BoltMessages messages) {
        this.shouldCloseConnectionInAutoCommitOnMessage(fsm, recorder, messages.rollback());
    }

    /**
     * Evaluates whether the connection is terminated when RESET is received while in auto commit
     * state.
     * <p />
     * RESET is only valid within INTERRUPTED state.
     */
    @StateMachineTest
    void shouldCloseConnectionInAutoCommitOnReset(
            @Autocommit StateMachine fsm, ResponseRecorder recorder, BoltMessages messages) {
        this.shouldCloseConnectionInAutoCommitOnMessage(fsm, recorder, messages.reset());
    }

    /**
     * Evaluates whether the connection is terminated when GOODBYE is received while in auto commit
     * state.
     * <p />
     * GOODBYE is only valid within READY state.
     */
    @StateMachineTest
    void shouldCloseConnectionInAutoCommitOnGoodbye(
            @Autocommit StateMachine fsm, ResponseRecorder recorder, BoltMessages messages) {
        this.shouldCloseConnectionInAutoCommitOnMessage(fsm, recorder, messages.goodbye());
    }

    private void shouldCloseConnectionInAutoCommitOnMessage(
            StateMachine fsm, ResponseRecorder recorder, RequestMessage message) {
        assertThat(fsm).shouldKillConnection(it -> it.process(message, recorder));

        assertThat(recorder).hasFailureResponse(Status.Request.Invalid);
    }
}
