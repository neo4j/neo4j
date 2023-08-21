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
package org.neo4j.bolt.testing.mock;

import org.mockito.Mockito;
import org.mockito.internal.util.MockUtil;
import org.neo4j.bolt.fsm.StateMachineConfiguration;
import org.neo4j.bolt.fsm.error.NoSuchStateException;
import org.neo4j.bolt.fsm.error.StateMachineException;
import org.neo4j.bolt.fsm.state.State;
import org.neo4j.bolt.fsm.state.StateReference;

public class StateMockFactory extends AbstractMockFactory<State, StateMockFactory> {
    private StateReference reference;

    private StateMockFactory(StateReference reference) {
        super(State.class);

        this.withReference(reference);
    }

    public static StateMockFactory newFactory(StateReference reference) {
        return new StateMockFactory(reference);
    }

    public static State newInstance(StateReference reference) {
        return newFactory(reference).build();
    }

    public static State attachNewInstance(StateReference reference, StateMachineConfiguration configuration) {
        return newFactory(reference).attachTo(configuration);
    }

    public State attachTo(StateMachineConfiguration configuration) {
        if (!MockUtil.isMock(configuration)) {
            throw new IllegalArgumentException("Expected FSM configuration mock but got "
                    + configuration.getClass().getName());
        }

        var mock = this.build();

        try {
            Mockito.doReturn(mock).when(configuration).lookup(this.reference);
        } catch (NoSuchStateException ignore) {
            // stubbing call
        }

        return mock;
    }

    public StateMockFactory withReference(StateReference reference) {
        this.reference = reference;

        return this.withStaticValue(State::reference, reference)
                .with(it -> Mockito.doCallRealMethod().when(it).name());
    }

    public StateMockFactory withResult(StateReference reference) {
        return this.withStaticValue(
                it -> {
                    try {
                        return it.process(Mockito.any(), Mockito.any(), Mockito.any());
                    } catch (StateMachineException ignore) {
                        // stubbing call
                        return null;
                    }
                },
                reference);
    }

    public StateMockFactory withResult(Throwable ex) {
        return this.with(it -> {
            try {
                Mockito.doThrow(ex).when(it).process(Mockito.any(), Mockito.any(), Mockito.any());
            } catch (StateMachineException ignore) {
                // stubbing call
            }
        });
    }
}
