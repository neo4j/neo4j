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
package org.neo4j.bolt.testing.assertions;

import static org.assertj.core.api.Assertions.fail;

import java.util.function.Consumer;
import org.assertj.core.api.AbstractAssert;
import org.assertj.core.api.Assertions;
import org.assertj.core.api.InstanceOfAssertFactory;
import org.neo4j.bolt.protocol.common.connector.connection.Connection;
import org.neo4j.bolt.protocol.common.fsm.AbstractStateMachine;
import org.neo4j.bolt.protocol.common.fsm.State;
import org.neo4j.bolt.protocol.common.fsm.StateMachine;
import org.neo4j.bolt.protocol.common.message.request.connection.ResetMessage;
import org.neo4j.bolt.protocol.v40.fsm.state.ReadyState;
import org.neo4j.bolt.runtime.BoltConnectionFatality;
import org.neo4j.bolt.testing.response.ResponseRecorder;
import org.neo4j.function.ThrowingConsumer;

public final class StateMachineAssertions extends AbstractAssert<StateMachineAssertions, StateMachine> {

    StateMachineAssertions(StateMachine stateMachine) {
        super(stateMachine, StateMachineAssertions.class);
    }

    public static StateMachineAssertions assertThat(StateMachine fsm) {
        return new StateMachineAssertions(fsm);
    }

    public static InstanceOfAssertFactory<StateMachine, StateMachineAssertions> stateMachine() {
        return new InstanceOfAssertFactory<>(StateMachine.class, StateMachineAssertions::new);
    }

    @SuppressWarnings("ConstantConditions")
    private AbstractStateMachine toAbstractStateMachine() {
        this.isNotNull();

        if (!(this.actual instanceof AbstractStateMachine)) {
            failWithMessage("Expected state machine to implement AbstractStateMachine");
        }

        return (AbstractStateMachine) this.actual;
    }

    public StateMachineAssertions stateSatisfies(Consumer<State> assertions) {
        this.isNotNull();

        var fsm = this.toAbstractStateMachine();
        assertions.accept(fsm.state());

        return this;
    }

    public StateMachineAssertions isInState(Class<? extends State> type) {
        return this.stateSatisfies(state -> Assertions.assertThat(state)
                .as("is in state %s", type.getSimpleName())
                .isInstanceOf(type));
    }

    public StateMachineAssertions isNotInState(Class<? extends State> type) {
        return this.stateSatisfies(state -> Assertions.assertThat(state)
                .as("is not in state %s", type.getSimpleName())
                .isNotInstanceOf(type));
    }

    public StateMachineAssertions isInInvalidState() {
        return this.stateSatisfies(state ->
                Assertions.assertThat(state).as("is not in a valid state").isNull());
    }

    public StateMachineAssertions canReset(Connection connection) {
        try {
            var recorder = new ResponseRecorder();

            connection.interrupt();
            this.actual.process(ResetMessage.getInstance(), recorder);

            ResponseRecorderAssertions.assertThat(recorder)
                    .as(this.descriptionText())
                    .hasSuccessResponse();

            this.isInState(ReadyState.class);
        } catch (BoltConnectionFatality ex) {
            fail("Failed to reset state machine", ex);
        }

        return this;
    }

    public StateMachineAssertions shouldKillConnection(
            ThrowingConsumer<StateMachine, BoltConnectionFatality> consumer) {
        this.isNotNull();

        try {
            consumer.accept(this.actual);
            fail("should have killed the connection");
        } catch (BoltConnectionFatality ignore) {
            // TODO: Exception likely no longer sensible
        }

        return this;
    }
}
