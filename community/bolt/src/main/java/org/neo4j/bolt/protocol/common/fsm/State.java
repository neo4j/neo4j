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
package org.neo4j.bolt.protocol.common.fsm;

import org.neo4j.bolt.protocol.common.message.request.RequestMessage;
import org.neo4j.bolt.runtime.BoltConnectionFatality;

/**
 * Represents a single state within the Bolt state machine.
 */
public interface State {

    /**
     * Retrieves a human-readable name via which this particular state is identified.
     *
     * @return a state name.
     */
    String name();

    /**
     * Processes a given request within a state machine and advances to a new state.
     *
     * @param message an arbitrary request.
     * @param context the context in which this request is handled.
     * @return a reference to the following state.
     * @throws BoltConnectionFatality when an unrecoverable error occurs.
     */
    State process(RequestMessage message, StateMachineContext context) throws BoltConnectionFatality;
}
