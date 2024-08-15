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

import java.util.Map;
import org.neo4j.bolt.fsm.error.NoSuchStateException;
import org.neo4j.bolt.fsm.state.State;
import org.neo4j.bolt.fsm.state.StateReference;
import org.neo4j.bolt.protocol.common.connector.connection.ConnectionHandle;
import org.neo4j.dbms.admissioncontrol.AdmissionControlService;
import org.neo4j.logging.internal.LogService;

final class ImmutableStateMachineConfiguration implements StateMachineConfiguration {
    private final State initialState;
    private final Map<StateReference, State> stateMap;

    public ImmutableStateMachineConfiguration(State initialState, Map<StateReference, State> stateMap) {
        this.initialState = initialState;
        this.stateMap = stateMap;
    }

    @Override
    public Factory builderOf() {
        var factory = new StateMachineFactoryImpl()
                .withInitialState(this.initialState.reference(), this.initialState.builderOf());

        this.stateMap.values().stream()
                .filter(state -> this.initialState != state)
                .forEach(state -> factory.withState(state.reference(), state.builderOf()));

        return factory;
    }

    @Override
    public StateReference initialState() {
        return this.initialState.reference();
    }

    @Override
    public State lookup(StateReference reference) throws NoSuchStateException {
        var state = this.stateMap.get(reference);
        if (state == null) {
            throw new NoSuchStateException(reference);
        }

        return state;
    }

    @Override
    public StateMachine createInstance(
            ConnectionHandle connection, LogService logService, AdmissionControlService admissionControlService) {
        return new StateMachineImpl(connection, this, logService, this.initialState, admissionControlService);
    }
}
