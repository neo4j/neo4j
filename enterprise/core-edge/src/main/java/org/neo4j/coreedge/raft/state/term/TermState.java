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
package org.neo4j.coreedge.raft.state.term;

import org.neo4j.coreedge.raft.log.RaftStorageException;

/**
 * Represents the current term for this Raft instance. Implementations of this interface are expected to
 * maintain the invariant that the term never transitions to a lower value.
 */
public interface TermState
{
    long currentTerm();

    /**
     * Updates the term to a new value. This value is generally expected, but not required, to be persisted. Consecutive
     * calls to this method should always have monotonically increasing arguments, thus maintaining the raft invariant
     * that the term is always non-decreasing. {@link IllegalArgumentException} can be thrown if an invalid value is
     * passed as argument.
     *
     * @param newTerm The new value.
     * @throws RaftStorageException If the implementation persists the state and a storage exception was raised.
     */
    void update( long newTerm ) throws RaftStorageException;
}
