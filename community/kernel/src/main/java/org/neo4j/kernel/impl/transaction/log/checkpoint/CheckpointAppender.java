/*
 * Copyright (c) 2002-2020 "Neo4j,"
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
package org.neo4j.kernel.impl.transaction.log.checkpoint;

import java.io.IOException;
import java.time.Instant;

import org.neo4j.kernel.impl.transaction.log.LogPosition;
import org.neo4j.kernel.impl.transaction.tracing.LogCheckPointEvent;

public interface CheckpointAppender
{
    /**
     * Appends a check point to a log which marks a starting point for recovery in the event of failure.
     * After this method have returned the check point mark must have been flushed to disk.
     *
     * @param logCheckPointEvent a trace event for the given check point operation.
     * @param logPosition the log position contained in the written check point
     * @param checkpointTime time when checkpoint occurred
     * @param reason reason for checkpoint to occur
     * @throws IOException if there was a problem appending the transaction. See method javadoc body for
     * how to handle exceptions in general thrown from this method.
     */
    void checkPoint( LogCheckPointEvent logCheckPointEvent, LogPosition logPosition, Instant checkpointTime, String reason ) throws IOException;
}
