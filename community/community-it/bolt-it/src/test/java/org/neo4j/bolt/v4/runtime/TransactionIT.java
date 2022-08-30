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
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.neo4j.bolt.testing.NullResponseHandler.nullResponseHandler;
import static org.neo4j.bolt.testing.assertions.AnyValueAssertions.assertThat;
import static org.neo4j.bolt.testing.assertions.MapValueAssertions.assertThat;
import static org.neo4j.bolt.testing.assertions.ResponseRecorderAssertions.assertThat;
import static org.neo4j.bolt.testing.assertions.StateMachineAssertions.assertThat;
import static org.neo4j.bolt.testing.messages.BoltV40Messages.begin;
import static org.neo4j.bolt.testing.messages.BoltV40Messages.commit;
import static org.neo4j.bolt.testing.messages.BoltV40Messages.discard;
import static org.neo4j.bolt.testing.messages.BoltV40Messages.pull;
import static org.neo4j.bolt.testing.messages.BoltV40Messages.reset;
import static org.neo4j.bolt.testing.messages.BoltV40Messages.rollback;
import static org.neo4j.bolt.testing.messages.BoltV40Messages.run;
import static org.neo4j.values.storable.Values.longValue;
import static org.neo4j.values.storable.Values.stringValue;
import static org.neo4j.values.virtual.VirtualValues.EMPTY_MAP;
import static org.neo4j.values.virtual.VirtualValues.list;

import java.util.regex.Pattern;
import org.junit.jupiter.api.Test;
import org.neo4j.bolt.protocol.common.fsm.StateMachine;
import org.neo4j.bolt.protocol.common.message.response.SuccessMessage;
import org.neo4j.bolt.runtime.BoltConnectionFatality;
import org.neo4j.bolt.runtime.BoltProtocolBreachFatality;
import org.neo4j.bolt.testing.response.ResponseRecorder;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.util.concurrent.BinaryLatch;
import org.neo4j.values.storable.TextValue;

class TransactionIT extends BoltStateMachineV4StateTestBase {
    private static final Pattern BOOKMARK_PATTERN =
            Pattern.compile("\\b[0-9a-f]{8}\\b-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-\\b[0-9a-f]{12}\\b:[0-9]+");

    @Test
    void shouldHandleBeginCommit() throws Throwable {
        // Given
        var recorder = new ResponseRecorder();
        var machine = newStateMachineAfterAuth();

        // When
        machine.process(begin(), recorder);

        machine.process(run("CREATE (n:InTx)"), recorder);
        machine.process(discard(), nullResponseHandler());

        machine.process(commit(), recorder);

        // Then
        assertThat(recorder).hasSuccessResponse(3);
    }

    @Test
    void shouldHandleBeginRollback() throws Throwable {
        // Given
        var recorder = new ResponseRecorder();
        var machine = newStateMachineAfterAuth();

        // When
        machine.process(begin(), recorder);

        machine.process(run("CREATE (n:InTx)"), recorder);
        machine.process(discard(), nullResponseHandler());
        machine.process(rollback(), recorder);

        // Then
        assertThat(recorder).hasSuccessResponse(3);
    }

    @Test
    void shouldFailWhenOutOfOrderRollbackInAutoCommitMode() throws Throwable {
        // Given
        var machine = newStateMachineAfterAuth();

        // When & Then
        assertThrows(BoltProtocolBreachFatality.class, () -> machine.process(rollback(), nullResponseHandler()));
    }

    @Test
    void shouldFailWhenOutOfOrderCommitInAutoCommitMode() throws Throwable {
        // Given
        var machine = newStateMachineAfterAuth();

        // When & Then
        assertThrows(BoltProtocolBreachFatality.class, () -> machine.process(commit(), nullResponseHandler()));
    }

    @Test
    void shouldReceiveBookmarkOnCommit() throws Throwable {
        // Given
        var recorder = new ResponseRecorder();
        var machine = newStateMachineAfterAuth();

        // When
        machine.process(begin(), nullResponseHandler());

        machine.process(run("CREATE (a:Person)"), nullResponseHandler());
        machine.process(discard(), nullResponseHandler());

        machine.process(commit(), recorder);

        // Then
        assertThat(recorder).hasSuccessResponse(meta -> assertThat(meta)
                .containsEntry("bookmark", value -> assertThat(value).asString().matches(BOOKMARK_PATTERN)));
    }

    @Test
    void shouldNotReceiveBookmarkOnRollback() throws Throwable {
        // Given
        var recorder = new ResponseRecorder();
        var machine = newStateMachineAfterAuth();

        // When
        machine.process(begin(), nullResponseHandler());

        machine.process(run("CREATE (a:Person)"), nullResponseHandler());
        machine.process(discard(), nullResponseHandler());

        machine.process(rollback(), recorder);

        // Then
        assertThat(recorder).hasSuccessResponse(meta -> assertThat(meta).doesNotContainKey("bookmark"));
    }

    @Test
    void shouldReadYourOwnWrites() throws Exception {
        var latch = new BinaryLatch();

        String bookmarkPrefix;
        try (var machine = newStateMachineAfterAuth()) {
            var recorder = new ResponseRecorder();
            machine.process(run("CREATE (n:A {prop:'one'})"), nullResponseHandler());
            machine.process(pull(), recorder);

            var response = recorder.next();

            assertThat(response.isResponse()).isTrue();

            var msg = response.asResponse();

            assertThat(msg).isInstanceOf(SuccessMessage.class);

            var bookmark = ((TextValue) ((SuccessMessage) msg).meta().get("bookmark")).stringValue();
            bookmarkPrefix = bookmark.split(":")[0];
        }

        var dbVersion = env.lastClosedTxId();
        var thread = new Thread(() -> {
            try (StateMachine machine = newStateMachineAfterAuth()) {
                latch.await();
                var recorder = new ResponseRecorder();
                machine.process(run("MATCH (n:A) SET n.prop = 'two'", EMPTY_MAP), nullResponseHandler());
                machine.process(pull(), recorder);
            } catch (Throwable connectionFatality) {
                throw new RuntimeException(connectionFatality);
            }
        });
        thread.start();

        var dbVersionAfterWrite = dbVersion + 1;
        try (var machine = newStateMachineAfterAuth()) {
            var recorder = new ResponseRecorder();
            latch.release();
            var bookmark = stringValue(bookmarkPrefix + ":" + dbVersionAfterWrite);

            machine.process(begin(env.databaseIdRepository(), list(bookmark)), recorder);
            machine.process(run("MATCH (n:A) RETURN n.prop"), recorder);
            machine.process(pull(), recorder);
            machine.process(commit(), recorder);

            assertThat(recorder)
                    .hasSuccessResponse(2)
                    .hasRecord(stringValue("two"))
                    .hasSuccessResponse()
                    .hasSuccessResponse(meta -> assertThat(meta).containsEntry("bookmark", value -> assertThat(value)
                            .asString()
                            .matches(BOOKMARK_PATTERN)));
        }

        thread.join();
    }

    @Test
    void shouldAllowNewRunAfterRunFailure() throws Throwable {
        // Given
        var machine = newStateMachineAfterAuth();
        var recorder = new ResponseRecorder();

        // When
        machine.process(run("INVALID QUERY"), recorder);
        machine.process(pull(), recorder);
        resetReceived(machine, recorder);
        machine.process(run("RETURN 2", EMPTY_MAP), recorder);
        machine.process(pull(), recorder);

        // Then
        assertThat(recorder)
                .hasFailureResponse(Status.Statement.SyntaxError)
                .hasIgnoredResponse()
                .hasSuccessResponse(2)
                .hasRecord(longValue(2))
                .hasSuccessResponse()
                .hasNoRemainingResponses();
    }

    @Test
    void shouldAllowNewRunAfterStreamingFailure() throws Throwable {
        // Given
        var machine = newStateMachineAfterAuth();
        var recorder = new ResponseRecorder();

        // When
        machine.process(run("UNWIND [1, 0] AS x RETURN 1 / x"), recorder);
        machine.process(pull(), recorder);
        resetReceived(machine, recorder);
        machine.process(run("RETURN 2"), recorder);
        machine.process(pull(), recorder);

        // Then
        assertThat(recorder)
                .hasSuccessResponse()
                .hasRecord(longValue(1))
                .hasFailureResponse(Status.Statement.ArithmeticError)
                .hasSuccessResponse(2)
                .hasRecord(longValue(2))
                .hasSuccessResponse()
                .hasNoRemainingResponses();
    }

    @Test
    void shouldNotAllowNewRunAfterRunFailure() throws Throwable {
        // Given
        var machine = newStateMachineAfterAuth();
        var recorder = new ResponseRecorder();

        // When
        machine.process(run("INVALID QUERY"), nullResponseHandler());
        machine.process(pull(), nullResponseHandler());

        // If I do not ack failure, then I shall not be able to do anything
        machine.process(run(), recorder);
        machine.process(pull(), recorder);

        // Then
        assertThat(recorder).hasIgnoredResponse(2).hasNoRemainingResponses();
    }

    @Test
    void shouldNotAllowNewRunAfterStreamingFailure() throws Throwable {
        // Given
        var machine = newStateMachineAfterAuth();
        var recorder = new ResponseRecorder();

        // When
        machine.process(run("UNWIND [1, 0] AS x RETURN 1 / x"), recorder);
        machine.process(pull(), recorder);

        // If I do not ack failure, then I shall not be able to do anything
        machine.process(run(), recorder);
        machine.process(pull(), recorder);

        // Then
        assertThat(recorder)
                .hasSuccessResponse()
                .hasRecord(longValue(1))
                .hasFailureResponse(Status.Statement.ArithmeticError)
                .hasIgnoredResponse(2)
                .hasNoRemainingResponses();
    }

    @Test
    void shouldNotAllowNewTransactionAfterProtocolFailure() throws Throwable {
        // You cannot recover from Protocol error.
        // Given
        var machine = newStateMachineAfterAuth();

        // When
        assertThat(machine)
                .shouldKillConnection(fsm -> fsm.process(commit(), nullResponseHandler()))
                .isInInvalidState();
    }

    private static void resetReceived(StateMachine machine, ResponseRecorder recorder) throws BoltConnectionFatality {
        machine.connection().interrupt();
        machine.interrupt();
        machine.process(reset(), recorder);
    }
}
