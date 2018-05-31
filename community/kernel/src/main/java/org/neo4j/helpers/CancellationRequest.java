/*
 * Copyright (c) 2002-2018 "Neo4j,"
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
package org.neo4j.helpers;

/**
 * Represents the concept of a cancellation notification towards a task. The implementation for the request will
 * remain application dependent, but the task to be cancelled can use this to discover if cancellation has been
 * requested.
 */
@Deprecated
public interface CancellationRequest
{
    /**
     * @return True iff a request for cancellation has been issued. It is assumed that the request cannot be withdrawn
     * so once this method returns true it must always return true on all subsequent calls.
     */
    @Deprecated
    boolean cancellationRequested();

    @Deprecated
    CancellationRequest NEVER_CANCELLED = () -> false;
}
