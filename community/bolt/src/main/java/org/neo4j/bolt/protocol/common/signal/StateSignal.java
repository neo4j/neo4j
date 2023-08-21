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
package org.neo4j.bolt.protocol.common.signal;

/**
 * Signals state changes to handlers within the network pipeline.
 */
public enum StateSignal {

    /**
     * Indicates that a job (e.g. a request) has begun processing within the state machine.
     */
    BEGIN_JOB_PROCESSING,

    /**
     * Indicates that a job (e.g. a request) has finished processing within the state machine.
     */
    END_JOB_PROCESSING,

    /**
     * Indicates that a state machine has entered a streaming state.
     *
     * @deprecated Provided explicitly for use with the STREAMING keep-alive mode which has been deprecated.
     */
    @Deprecated(forRemoval = true)
    ENTER_STREAMING,

    /**
     * Indicates that a state machine has left a streaming state.
     *
     * @deprecated Provided explicitly for use with the STREAMING keep-alive mode which has been deprecated.
     */
    @Deprecated(forRemoval = true)
    EXIT_STREAMING
}
