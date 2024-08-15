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

import java.util.function.Consumer;
import org.neo4j.bolt.fsm.error.NoSuchStateException;
import org.neo4j.bolt.fsm.state.State;
import org.neo4j.bolt.fsm.state.StateReference;
import org.neo4j.bolt.fsm.state.transition.StateTransition;
import org.neo4j.bolt.protocol.common.connector.connection.ConnectionHandle;
import org.neo4j.dbms.admissioncontrol.AdmissionControlService;
import org.neo4j.logging.internal.LogService;

/**
 * Encapsulates the configuration of a state machine.
 * <p />
 * Instances of this interface are generally immutable and will be allocated by
 * {@link org.neo4j.bolt.protocol.common.BoltProtocol protocol definitions} based on its respective
 * capabilities and constraints.
 * <p />
 * Unless explicitly specified, none of the methods within this interface should be considered
 * thread-safe.
 */
public interface StateMachineConfiguration {

    /**
     * Creates a new empty state machine factory.
     *
     * @return a new state machine factory.
     */
    static Factory builder() {
        return new StateMachineFactoryImpl();
    }

    /**
     * Creates a new state machine factory based on the states and transitions present within this
     * state machine.
     *
     * @return a new state machine factory.
     */
    Factory builderOf();

    /**
     * Retrieves a reference to the initial state in which new state machine instances will be
     * spawned.
     * <p />
     * This state is also used as the {@link StateMachine#defaultState() default state} to which an
     * instance will {@link Context#reset() reset}.
     *
     * @return a state reference.
     */
    StateReference initialState();

    /**
     * Performs a lookup for a given state within this state machine based on its unique reference.
     *
     * @param reference a state reference.
     * @return a state.
     * @throws NoSuchStateException when the given state is not present within this state machine
     *                              configuration.
     */
    State lookup(StateReference reference) throws NoSuchStateException;

    /**
     * Initializes a new state machine context for a given connection.
     *
     * @param connection a connection.
     * @return a state machine context.
     */
    StateMachine createInstance(
            ConnectionHandle connection, LogService logService, AdmissionControlService admissionControlService);

    interface Factory {

        /**
         * Constructs a new state machine based on the configuration present within this factory.
         *
         * @return a state machine configuration.
         */
        StateMachineConfiguration build();

        /**
         * Selects a pre-existing state as the initial state for new state machine instances.
         *
         * @param reference a state reference via which the desired state is identified.
         */
        Factory withInitialState(StateReference reference);

        /**
         * Creates a state with the given desired name which shall act as an initial state for new
         * state machine instances.
         *
         * @param reference a state reference via which the new state shall be identified.
         * @param factoryConsumer a consumer function with configuration logic for the state.
         */
        Factory withInitialState(StateReference reference, Consumer<State.Factory> factoryConsumer);

        default Factory withInitialState(StateReference reference, StateTransition<?>... transitions) {
            return this.withInitialState(reference, state -> {
                for (var transition : transitions) {
                    state.withTransition(transition);
                }
            });
        }

        /**
         * Creates a state with the given desired name.
         *
         * @param reference a state reference via which the new state shall be identified.
         * @param factoryConsumer a consumer function with configuration logic for the state.
         */
        Factory withState(StateReference reference, Consumer<State.Factory> factoryConsumer);

        /**
         * Creates a state with the given name and a static set of transitions.
         * @param reference a state reference via which the new state shall be identified.
         * @param transitions a set of transitions via which the state machine may transition out of
         *                    the new state.
         * @return a reference to this factory instance.
         */
        default Factory withState(StateReference reference, StateTransition<?>... transitions) {
            return this.withState(reference, state -> {
                for (var transition : transitions) {
                    state.withTransition(transition);
                }
            });
        }

        /**
         * Removes a state with the given name.
         *
         * @param reference a state reference.
         */
        Factory withoutState(StateReference reference);
    }
}
