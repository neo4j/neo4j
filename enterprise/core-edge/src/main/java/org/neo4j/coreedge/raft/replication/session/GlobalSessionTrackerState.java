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
package org.neo4j.coreedge.raft.replication.session;

/**
 * Each cluster instance has a global session as well as several local sessions. Each local session
 * tracks its operation by assigning a unique sequence number to each operation. This allows
 * an operation originating from an instance to be uniquely identified and duplicate attempts
 * at performing that operation can be filtered out.
 * <p>
 * The session tracker defines the strategy for which local operations are allowed to be performed
 * and the strategy is to only allow operations to occur in strict order, that is with no gaps,
 * starting with sequence number zero. This is done for reasons of efficiency and creates a very
 * direct coupling between session tracking and operation validation. This class is in charge
 * of both.
 */
public interface GlobalSessionTrackerState<MEMBER>
{
    /**
     * Tracks the operation and returns true iff this operation should be allowed.
     */
    boolean validateOperation( GlobalSession<MEMBER> globalSession, LocalOperationId localOperationId );

    void update( GlobalSession<MEMBER> globalSession, LocalOperationId localOperationId, long logIndex );

    long logIndex();
}
