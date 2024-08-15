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
package org.neo4j.bolt.protocol.common.fsm;

import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Timeout;
import org.neo4j.bolt.fsm.StateMachine;
import org.neo4j.bolt.fsm.error.StateMachineException;
import org.neo4j.bolt.protocol.common.message.request.connection.RouteMessage;
import org.neo4j.bolt.protocol.common.message.request.streaming.DiscardMessage;
import org.neo4j.bolt.protocol.common.message.request.streaming.PullMessage;
import org.neo4j.bolt.protocol.common.message.request.transaction.BeginMessage;
import org.neo4j.bolt.protocol.common.message.request.transaction.CommitMessage;
import org.neo4j.bolt.protocol.common.message.request.transaction.RollbackMessage;
import org.neo4j.bolt.protocol.common.message.request.transaction.RunMessage;
import org.neo4j.bolt.testing.annotation.Version;
import org.neo4j.bolt.testing.annotation.fsm.StateMachineTest;
import org.neo4j.bolt.testing.annotation.fsm.StateMachineTestExtension;
import org.neo4j.bolt.testing.annotation.fsm.initializer.Authenticated;
import org.neo4j.bolt.testing.annotation.fsm.initializer.InTransaction;
import org.neo4j.bolt.testing.annotation.fsm.initializer.Negotiated;
import org.neo4j.bolt.testing.annotation.fsm.initializer.mock.MockAutocommit;
import org.neo4j.bolt.testing.annotation.fsm.initializer.mock.MockStreaming;
import org.neo4j.bolt.testing.assertions.ResponseRecorderAssertions;
import org.neo4j.bolt.testing.assertions.StateMachineAssertions;
import org.neo4j.bolt.testing.messages.BoltMessages;
import org.neo4j.bolt.testing.mock.StatementMockFactory;
import org.neo4j.bolt.testing.mock.TransactionManagerMockFactory;
import org.neo4j.bolt.testing.mock.TransactionMockFactory;
import org.neo4j.bolt.testing.response.ResponseRecorder;
import org.neo4j.bolt.tx.TransactionManager;
import org.neo4j.values.virtual.MapValueBuilder;
import org.neo4j.values.virtual.VirtualValues;

/**
 * Ensures that state machines transition through the full set of possible transitions as expected
 * while updating the configured default state where necessary.
 */
@StateMachineTestExtension
public class StateTransitionStateMachineTest {

    /**
     * Ensures that newly created state machines spawn within the
     * {@link States#NEGOTIATION NEGOTIATION} state.
     */
    @StateMachineTest(since = @Version(major = 5, minor = 1))
    void shouldBeInNegotiationStateWhenCreated(StateMachine fsm) {
        StateMachineAssertions.assertThat(fsm).isInState(States.NEGOTIATION).hasDefaultState(States.NEGOTIATION);
    }

    @StateMachineTest(until = @Version(major = 5, minor = 1))
    void shouldBeInAuthenticationStateWhenCreated(StateMachine fsm) {
        StateMachineAssertions.assertThat(fsm).isInState(States.AUTHENTICATION).hasDefaultState(States.AUTHENTICATION);
    }

    /**
     * Ensures that state machines transition from {@link States#NEGOTIATION NEGOTIATION} to
     * {@link States#AUTHENTICATION AUTHENTICATION} when configured for protocol versions with
     * distinct authentication stages.
     * <p />
     * Also verifies that the configured default state advances to
     * {@link States#AUTHENTICATION AUTHENTICATION} in order to facilitate safe processing of
     * {@link StateMachine#reset() RESET} commands.
     */
    @StateMachineTest(since = @Version(major = 5, minor = 1))
    void shouldTransitionFromNegotiationToAuthenticationOnHello(
            StateMachine fsm, BoltMessages messages, ResponseRecorder recorder) throws StateMachineException {
        fsm.process(messages.hello(), recorder, null);

        ResponseRecorderAssertions.assertThat(recorder).hasSuccessResponse();

        StateMachineAssertions.assertThat(fsm).isInState(States.AUTHENTICATION).hasDefaultState(States.AUTHENTICATION);
    }

    /**
     * Ensures that state machines transition from {@link States#AUTHENTICATION AUTHENTICATION} to
     * {@link States#READY READY} when configured for protocol versions with distinct authentication
     * stages.
     * <p />
     * Also verifies that the configured default state advances to {@link States#READY READY} in
     * order to facilitate safe processing of {@link StateMachine#reset() RESET} commands.
     */
    @StateMachineTest(since = @Version(major = 5, minor = 1))
    void shouldTransitionFromAuthenticationToReadyOnLogon(
            @Negotiated StateMachine fsm, BoltMessages messages, ResponseRecorder recorder)
            throws StateMachineException {
        fsm.process(messages.logon(), recorder, null);

        ResponseRecorderAssertions.assertThat(recorder).hasSuccessResponse();

        StateMachineAssertions.assertThat(fsm).isInState(States.READY).hasDefaultState(States.READY);
    }

    /**
     * Ensures that state machines transition from {@link States#READY READY} to
     * {@link States#AUTHENTICATION AUTHENTICATION} when configured for protocol versions with
     * distinct authentication stages.
     * <p />
     * Also verifies that the configured default state advances to
     * {@link States#AUTHENTICATION AUTHENTICATION} in order to facilitate safe processing of
     * {@link StateMachine#reset() RESET} commands.
     */
    @StateMachineTest(since = @Version(major = 5, minor = 1))
    void shouldTransitionFromReadyToAuthenticationOnLogoff(
            @Authenticated StateMachine fsm, BoltMessages messages, ResponseRecorder recorder)
            throws StateMachineException {
        fsm.process(messages.logoff(), recorder, null);

        ResponseRecorderAssertions.assertThat(recorder).hasSuccessResponse();

        StateMachineAssertions.assertThat(fsm).isInState(States.AUTHENTICATION).hasDefaultState(States.AUTHENTICATION);
    }

    /**
     * Ensures that state machines transition from {@link States#NEGOTIATION AUTHENTICATION} to
     * {@link States#READY READY} when configured for protocol versions which lack distinct
     * authentication stages.
     * <p />
     * Also verifies that the configured default state advances to {@link States#READY READY} in
     * order to facilitate safe processing of {@link StateMachine#reset() RESET} commands.
     */
    @StateMachineTest(until = @Version(major = 5, minor = 1))
    void shouldTransitionFromNegotiationToReadyOnHello(
            StateMachine fsm, BoltMessages messages, ResponseRecorder recorder) throws StateMachineException {
        fsm.process(messages.hello(), recorder, null);

        ResponseRecorderAssertions.assertThat(recorder).hasSuccessResponse();

        StateMachineAssertions.assertThat(fsm).isInState(States.READY).hasDefaultState(States.READY);
    }

    /**
     * Ensures that state machines remain within {@link States#READY READY} when
     * executing {@link RouteMessage ROUTE}.
     */
    @Timeout(value = 2, unit = TimeUnit.MINUTES)
    @StateMachineTest(since = @Version(major = 4, minor = 3))
    void shouldRemainInReadyOnRoute(
            @Authenticated StateMachine fsm,
            BoltMessages messages,
            ResponseRecorder recorder,
            TransactionManager transactionManager)
            throws StateMachineException {
        var builder = new MapValueBuilder();
        builder.add("rt", VirtualValues.EMPTY_LIST);
        var result = builder.build();

        TransactionManagerMockFactory.newFactory()
                .withFactory((type, owner, databaseName, mode, bookmarks, timeout, metadata) ->
                        TransactionMockFactory.newFactory()
                                .withFactory((statement, params) -> StatementMockFactory.newFactory()
                                        .withResults(result)
                                        .build())
                                .build())
                .apply(transactionManager);

        fsm.process(messages.route(), recorder, null);

        ResponseRecorderAssertions.assertThat(recorder).hasSuccessResponse();

        StateMachineAssertions.assertThat(fsm).isInState(States.READY).hasDefaultState(States.READY);
    }

    /**
     * Ensures that state machines transition from {@link States#READY READY} to
     * {@link States#AUTO_COMMIT AUTO_COMMIT} when executing {@link RunMessage RUN}.
     */
    @StateMachineTest
    void shouldTransitionFromReadyToAutocommitOnRun(
            @Authenticated StateMachine fsm, BoltMessages messages, ResponseRecorder recorder)
            throws StateMachineException {
        fsm.process(messages.run("RETURN 1"), recorder, null);

        ResponseRecorderAssertions.assertThat(recorder).hasSuccessResponse();

        StateMachineAssertions.assertThat(fsm).isInState(States.AUTO_COMMIT).hasDefaultState(States.READY);
    }

    /**
     * Ensures that state machines remain within {@link States#AUTO_COMMIT AUTO_COMMIT} when
     * executing {@link PullMessage PULL} while results remain within the statement once the
     * operation completes.
     */
    @StateMachineTest
    void shouldRemainInAutocommitOnPullWhenResultsRemain(
            @MockAutocommit(results = 10) StateMachine fsm, BoltMessages messages, ResponseRecorder recorder)
            throws StateMachineException {
        fsm.process(messages.pull(5), recorder, null);

        ResponseRecorderAssertions.assertThat(recorder).hasRecords(5).hasSuccessResponse();

        StateMachineAssertions.assertThat(fsm).isInState(States.AUTO_COMMIT).hasDefaultState(States.READY);
    }

    /**
     * Ensures that state machines transition from {@link States#AUTO_COMMIT AUTO_COMMIT} to
     * {@link States#READY READY} when executing {@link PullMessage PULL} while no results remain
     * within the statement once the operation completes.
     */
    @StateMachineTest
    void shouldTransitionFromAutocommitToReadyOnPullWhenNoResultsRemain(
            @MockAutocommit StateMachine fsm, BoltMessages messages, ResponseRecorder recorder)
            throws StateMachineException {
        fsm.process(messages.pull(), recorder, null);

        ResponseRecorderAssertions.assertThat(recorder).hasRecord().hasSuccessResponse();

        StateMachineAssertions.assertThat(fsm).isInState(States.READY).hasDefaultState(States.READY);
    }

    /**
     * Ensures that state machines remain within {@link States#AUTO_COMMIT AUTO_COMMIT} when
     * executing {@link DiscardMessage DISCARD} while results remain within the statement once the
     * operation completes.
     */
    @StateMachineTest
    void shouldRemainInAutocommitOnDiscardWhenResultsRemain(
            @MockAutocommit(results = 10) StateMachine fsm, BoltMessages messages, ResponseRecorder recorder)
            throws StateMachineException {
        fsm.process(messages.discard(5), recorder, null);

        ResponseRecorderAssertions.assertThat(recorder).hasSuccessResponse();

        StateMachineAssertions.assertThat(fsm).isInState(States.AUTO_COMMIT).hasDefaultState(States.READY);
    }

    /**
     * Ensures that state machines transition from {@link States#AUTO_COMMIT AUTO_COMMIT} to
     * {@link States#READY READY} when executing {@link DiscardMessage DISCARD} while no results
     * remain within the statement once the operation completes.
     */
    @StateMachineTest
    void shouldTransitionFromAutocommitToReadyOnDiscardWhenNoResultsRemain(
            @MockAutocommit StateMachine fsm, BoltMessages messages, ResponseRecorder recorder)
            throws StateMachineException {
        fsm.process(messages.discard(), recorder, null);

        ResponseRecorderAssertions.assertThat(recorder).hasSuccessResponse();

        StateMachineAssertions.assertThat(fsm).isInState(States.READY).hasDefaultState(States.READY);
    }

    /**
     * Ensures that state machines transition from {@link States#READY READY} to
     * {@link States#IN_TRANSACTION IN_TRANSACTION} when executing {@link BeginMessage BEGIN}.
     */
    @StateMachineTest
    void shouldTransitionFromReadyToInTransactionOnBegin(
            @Authenticated StateMachine fsm, BoltMessages messages, ResponseRecorder recorder)
            throws StateMachineException {
        fsm.process(messages.begin(), recorder, null);

        ResponseRecorderAssertions.assertThat(recorder).hasSuccessResponse();

        StateMachineAssertions.assertThat(fsm).isInState(States.IN_TRANSACTION).hasDefaultState(States.READY);
    }

    /**
     * Ensures that state machines remain within {@link States#IN_TRANSACTION IN_TRANSACTION} when
     * executing {@link RunMessage RUN}.
     */
    @StateMachineTest
    void shouldRemainInInTransactionOnRun(
            @InTransaction StateMachine fsm, BoltMessages messages, ResponseRecorder recorder)
            throws StateMachineException {
        fsm.process(messages.run("RETURN 1"), recorder, null);

        ResponseRecorderAssertions.assertThat(recorder).hasSuccessResponse();

        StateMachineAssertions.assertThat(fsm).isInState(States.IN_TRANSACTION).hasDefaultState(States.READY);
    }

    /**
     * Ensures that state machines remain within {@link States#IN_TRANSACTION IN_TRANSACTION} when
     * executing {@link PullMessage PULL} while results remain within the active statement once the
     * operation completes.
     */
    @StateMachineTest
    void shouldRemainInInTransactionOnPullWhenResultsRemain(
            @MockStreaming(results = 10) StateMachine fsm, BoltMessages messages, ResponseRecorder recorder)
            throws StateMachineException {
        fsm.process(messages.pull(5), recorder, null);

        ResponseRecorderAssertions.assertThat(recorder).hasRecords(5).hasSuccessResponse();

        StateMachineAssertions.assertThat(fsm).isInState(States.IN_TRANSACTION).hasDefaultState(States.READY);
    }

    /**
     * Ensures that state machines remain within {@link States#IN_TRANSACTION IN_TRANSACTION} when
     * executing {@link PullMessage PULL} when no results remain within the statement once the
     * operation completes.
     */
    @StateMachineTest
    void shouldRemainInInTransactionOnPullWhenNoResultsRemain(
            @MockStreaming StateMachine fsm, BoltMessages messages, ResponseRecorder recorder)
            throws StateMachineException {
        fsm.process(messages.pull(), recorder, null);

        ResponseRecorderAssertions.assertThat(recorder).hasRecords(1).hasSuccessResponse();

        StateMachineAssertions.assertThat(fsm).isInState(States.IN_TRANSACTION).hasDefaultState(States.READY);
    }

    /**
     * Ensures that state machines transition from {@link States#IN_TRANSACTION IN_TRANSACTION} to
     * {@link States#READY READY} when executing {@link CommitMessage COMMIT} while no active
     * statements remain.
     */
    @StateMachineTest
    void shouldTransitionFromInTransactionToReadyOnCommitWhenNoResultsRemain(
            @InTransaction StateMachine fsm, BoltMessages messages, ResponseRecorder recorder)
            throws StateMachineException {
        fsm.process(messages.commit(), recorder, null);

        ResponseRecorderAssertions.assertThat(recorder).hasSuccessResponse();

        StateMachineAssertions.assertThat(fsm).isInState(States.READY).hasDefaultState(States.READY);
    }

    /**
     * Ensures that state machines transition from {@link States#IN_TRANSACTION IN_TRANSACTION} to
     * {@link States#READY READY} when executing {@link CommitMessage COMMIT} while active
     * statements remain.
     */
    @StateMachineTest
    void shouldTransitionFromInTransactionToReadyOnCommitWhenResultsRemain(
            @MockStreaming StateMachine fsm, BoltMessages messages, ResponseRecorder recorder)
            throws StateMachineException {
        fsm.process(messages.commit(), recorder, null);

        ResponseRecorderAssertions.assertThat(recorder).hasSuccessResponse();

        StateMachineAssertions.assertThat(fsm).isInState(States.READY).hasDefaultState(States.READY);
    }

    /**
     * Ensures that state machines transition from {@link States#IN_TRANSACTION IN_TRANSACTION} to
     * {@link States#READY READY} when executing {@link RollbackMessage ROLLBACK} while no active
     * statements remain.
     */
    @StateMachineTest
    void shouldTransitionFromInTransactionToReadyOnRollbackWhenNoResultsRemain(
            @InTransaction StateMachine fsm, BoltMessages messages, ResponseRecorder recorder)
            throws StateMachineException {
        fsm.process(messages.rollback(), recorder, null);

        ResponseRecorderAssertions.assertThat(recorder).hasSuccessResponse();

        StateMachineAssertions.assertThat(fsm).isInState(States.READY).hasDefaultState(States.READY);
    }

    /**
     * Ensures that state machine transition from {@link States#IN_TRANSACTION IN_TRANSACTION} to
     * {@link States#READY READY} when executing {@link RollbackMessage ROLLBACK} while active
     * statements remain.
     */
    @StateMachineTest
    void shouldTransitionFromInTransactionToReadyOnRollbackWhenResultsRemain(
            @MockStreaming StateMachine fsm, BoltMessages messages, ResponseRecorder recorder)
            throws StateMachineException {
        fsm.process(messages.rollback(), recorder, null);

        ResponseRecorderAssertions.assertThat(recorder).hasSuccessResponse();

        StateMachineAssertions.assertThat(fsm).isInState(States.READY).hasDefaultState(States.READY);
    }
}
