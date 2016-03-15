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
package org.neo4j.coreedge.raft;

/**
 * The RAFT external entity that is interested in log entries and
 * typically applies them to a state machine.
 */
public interface RaftStateMachine
{
    /**
     * Called when the highest committed index increases.
     */
    default void notifyCommitted( long commitIndex ) {}

    /**
     * Download and install a snapshot of state from another member of the cluster.
     * <p/>
     * Called when the consensus system no longer has the log entries required to further update the state machine,
     * because they have been deleted through pruning.
     */
    default void downloadSnapshot() {}
}
