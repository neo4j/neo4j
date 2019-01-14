/*
 * Copyright (c) 2002-2019 "Neo4j,"
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
package org.neo4j.kernel.api.query;

import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.neo4j.kernel.impl.locking.ActiveLock;

/**
 * Internal representation of the status of an executing query.
 * <p>
 * This is used for inspecting the state of a query.
 *
 * @see ExecutingQuery#status
 */
abstract class ExecutingQueryStatus
{
    static final String PLANNING_STATE = "planning";
    static final String RUNNING_STATE = "running";
    static final String WAITING_STATE = "waiting";
    /**
     * Time in nanoseconds that has been spent waiting in the current state.
     * This is the portion of wait time not included in the {@link ExecutingQuery#waitTimeNanos} field.
     *
     * @param currentTimeNanos
     *         the current timestamp on the nano clock.
     * @return the time between the time this state started waiting and the provided timestamp.
     */
    abstract long waitTimeNanos( long currentTimeNanos );

    abstract Map<String,Object> toMap( long currentTimeNanos );

    abstract String name();

    boolean isPlanning()
    {
        return false;
    }

    /**
     * Is query waiting on a locks
     * @return true if waiting on locks, false otherwise
     */
    boolean isWaitingOnLocks()
    {
        return false;
    }

    /**
     * List of locks query is waiting on. Will be empty for all of the statuses except for {@link WaitingOnLock}.
     * @return list of locks query is waiting on, empty list if query is not waiting.
     */
    List<ActiveLock> waitingOnLocks()
    {
        return Collections.emptyList();
    }
}
