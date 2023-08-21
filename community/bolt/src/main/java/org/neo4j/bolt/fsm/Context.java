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

import io.netty.channel.ChannelFuture;
import java.time.Clock;
import org.neo4j.bolt.fsm.error.NoSuchStateException;
import org.neo4j.bolt.fsm.state.State;
import org.neo4j.bolt.fsm.state.StateReference;
import org.neo4j.bolt.protocol.common.connector.connection.Connection;

public interface Context extends StateMachine {

    /**
     * Retrieves the state machine which manages the states and transitions followed by this context.
     *
     * @return
     */
    StateMachineConfiguration configuration();

    /**
     * Retrieves the state in which this state machine currently resides.
     *
     * @return a state machine state.
     */
    StateReference state();

    /**
     * Shorthand for {@link StateMachineConfiguration#lookup(StateReference)}
     *
     * @param reference a state reference.
     * @return a state.
     * @throws NoSuchStateException when no state with the given reference exists within the state
     *                              machine configuration.
     * @see StateMachineConfiguration#lookup(StateReference)
     */
    State lookup(StateReference reference) throws NoSuchStateException;

    /**
     * Shorthand for {@link Connection#clock()}.
     *
     * @see Connection#clock()
     */
    default Clock clock() {
        return connection().clock();
    }

    /**
     * Shorthand for {@link Connection#write(Object)}
     *
     * @see Connection#write(Object)
     */
    default ChannelFuture write(Object message) {
        return connection().write(message);
    }
}
