/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.causalclustering.core.replication;

import org.neo4j.causalclustering.core.state.Result;

/**
 * Keeps track of operations in progress. Operations move through two phases:
 *  - waiting for replication
 *  - waiting for result
 */
public interface ProgressTracker
{
    /**
     * Called to start tracking the progress of an operation.
     *
     * @param operation The operation to track.
     *
     * @return A container for the progress.
     */
    Progress start( DistributedOperation operation );

    /**
     * Called when an operation has been replicated and is waiting
     * for the operation to be locally applied.
     *
     * @param operation The operation that has been replicated.
     */
    void trackReplication( DistributedOperation operation );

    /**
     * Called when an operation has been applied and a result is
     * available.
     *
     * @param operation The operation that has been applied.
     * @param result The result of the operation.
     */
    void trackResult( DistributedOperation operation, Result result );

    /**
     * Called when an operation should be abnormally aborted
     * and removed from the tracker.
     *
     * @param operation The operation to be aborted.
     */
    void abort( DistributedOperation operation );

    /**
     * Called when a significant event related to replication
     * has occurred (i.e. leader switch).
     */
    void triggerReplicationEvent();

    /**
     * Returns a count of the current number of in-progress tracked operations.
     *
     * @return A count of currently tracked operations..
     */
    int inProgressCount();
}
