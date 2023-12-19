/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * Neo4j Sweden Software License, as found in the associated LICENSE.txt
 * file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * Neo4j Sweden Software License for more details.
 */
package org.neo4j.causalclustering.core.state.machines;

import java.io.IOException;
import java.util.function.Consumer;

import org.neo4j.causalclustering.core.state.Result;

public interface StateMachine<Command>
{
    /**
     * Apply command to state machine, modifying its internal state.
     * Implementations should be idempotent, so that the caller is free to replay commands from any point in the log.
     * @param command Command to the state machine.
     * @param commandIndex The index of the command.
     * @param callback To be called when a result is produced.
     */
    void applyCommand( Command command, long commandIndex, Consumer<Result> callback );

    /**
     * Flushes state to durable storage.
     * @throws IOException
     */
    void flush() throws IOException;

    /**
     * Return the index of the last applied command by this state machine.
     * @return the last applied index for this state machine
     */
    long lastAppliedIndex();
}
