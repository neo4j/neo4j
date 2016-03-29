/*
 * Copyright (c) 2002-2016 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.coreedge.raft.state;

import java.io.IOException;
import java.util.Optional;

public interface StateMachine<Command>
{
    /**
     * Apply command to state machine, modifying its internal state.
     * Implementations should be idempotent, so that the caller is free to replay commands from any point in the log.
     *  @param command Command to the state machine.
     * @param commandIndex The index of the command.
     */
    Optional<Result> applyCommand( Command command, long commandIndex );

    /**
     * Flushes state to durable storage.
     * @throws IOException
     */
    void flush() throws IOException;
}
