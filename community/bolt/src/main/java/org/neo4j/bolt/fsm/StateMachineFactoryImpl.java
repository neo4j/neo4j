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

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.neo4j.bolt.fsm.StateMachineConfiguration.Factory;
import org.neo4j.bolt.fsm.state.State;
import org.neo4j.bolt.fsm.state.StateReference;

final class StateMachineFactoryImpl implements Factory {

    private StateReference initialState;
    private final Map<StateReference, State.Factory> stateMap = new HashMap<>();

    @Override
    public ImmutableStateMachineConfiguration build() {
        if (this.initialState == null) {
            throw new IllegalStateException("No initial state configured");
        }

        var stateMap = this.stateMap.values().stream()
                .map(State.Factory::build)
                .collect(Collectors.toMap(State::reference, Function.identity()));
        var initialState = stateMap.get(this.initialState);

        return new ImmutableStateMachineConfiguration(initialState, stateMap);
    }

    @Override
    public Factory withInitialState(StateReference reference) {
        if (!this.stateMap.containsKey(reference)) {
            throw new IllegalArgumentException("No such state: " + reference);
        }

        this.initialState = reference;
        return this;
    }

    @Override
    public StateMachineFactoryImpl withInitialState(StateReference reference, Consumer<State.Factory> factoryConsumer) {
        this.withState(reference, factoryConsumer);
        this.initialState = reference;
        return this;
    }

    public StateMachineFactoryImpl withInitialState(StateReference reference, State.Factory factory) {
        this.withState(reference, factory);
        this.initialState = factory.reference();
        return this;
    }

    @Override
    public StateMachineFactoryImpl withState(StateReference reference, Consumer<State.Factory> factoryConsumer) {
        var factory = this.stateMap.computeIfAbsent(reference, State::builder);
        factoryConsumer.accept(factory);
        return this;
    }

    public StateMachineFactoryImpl withState(StateReference reference, State.Factory factory) {
        this.stateMap.put(reference, factory);
        return this;
    }

    @Override
    public StateMachineFactoryImpl withoutState(StateReference reference) {
        this.stateMap.remove(reference);
        if (Objects.equals(this.initialState, reference)) {
            this.initialState = null;
        }

        return this;
    }
}
