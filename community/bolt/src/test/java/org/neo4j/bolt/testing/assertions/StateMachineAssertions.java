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
package org.neo4j.bolt.testing.assertions;

import static org.assertj.core.api.Assertions.fail;

import java.util.function.Consumer;
import org.assertj.core.api.AbstractAssert;
import org.assertj.core.api.Assertions;
import org.assertj.core.api.InstanceOfAssertFactory;
import org.neo4j.bolt.fsm.Context;
import org.neo4j.bolt.fsm.StateMachine;
import org.neo4j.bolt.fsm.error.StateMachineException;
import org.neo4j.bolt.fsm.state.StateReference;
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

    public StateMachineAssertions stateSatisfies(Consumer<StateReference> assertions) {
        this.isNotNull();

        assertions.accept(((Context) this.actual).state());

        return this;
    }

    public StateMachineAssertions defaultStateSatisfies(Consumer<StateReference> assertions) {
        this.isNotNull();

        assertions.accept(this.actual.defaultState());

        return this;
    }

    public StateMachineAssertions isInState(StateReference reference) {
        return this.stateSatisfies(state -> Assertions.assertThat(state)
                .as("is in state %s", reference.name())
                .isEqualTo(reference));
    }

    public StateMachineAssertions isNotInState(StateReference reference) {
        return this.stateSatisfies(state -> Assertions.assertThat(state)
                .as("is not in state %s", reference.name())
                .isEqualTo(reference));
    }

    public StateMachineAssertions hasDefaultState(StateReference reference) {
        return this.defaultStateSatisfies(state -> Assertions.assertThat(state)
                .as("is not configured with default state %s", reference.name())
                .isEqualTo(reference));
    }

    public StateMachineAssertions isInterrupted() {
        this.isNotNull();

        if (!this.actual.isInterrupted()) {
            this.failWithMessage("Expected state machine to be interrupted");
        }

        return this;
    }

    public StateMachineAssertions isNotInterrupted() {
        this.isNotNull();

        if (this.actual.isInterrupted()) {
            this.failWithMessage("Expected state machine to not be interrupted");
        }

        return this;
    }

    public StateMachineAssertions hasFailed() {
        this.isNotNull();

        if (!this.actual.hasFailed()) {
            failWithMessage("Expected state machine to be marked failed");
        }

        return this;
    }

    public StateMachineAssertions hasNotFailed() {
        this.isNotNull();

        if (this.actual.hasFailed()) {
            failWithMessage("Expected state machine to not be marked failed");
        }

        return this;
    }

    public StateMachineAssertions shouldKillConnection(ThrowingConsumer<StateMachine, StateMachineException> consumer) {
        this.isNotNull();

        try {
            consumer.accept(this.actual);
            fail("should have killed the connection");
        } catch (StateMachineException ignore) {
        }

        return this;
    }
}
