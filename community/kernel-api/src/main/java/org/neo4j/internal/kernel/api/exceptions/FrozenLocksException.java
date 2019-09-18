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
package org.neo4j.internal.kernel.api.exceptions;

import org.neo4j.kernel.api.exceptions.Status;

/**
 * Indicates that some code attempted to acquire or release a Lock while lock interactions were frozen.
 *
 * @see Transaction#freezeLocks()
 */
public class FrozenLocksException extends RuntimeException implements Status.HasStatus
{
    public FrozenLocksException( int lockSessionId )
    {
        super( String.format( "A interaction with a frozen lock client has occurred on lock session %d, possibly by " +
                              "concurrent access to the transaction.", lockSessionId ) );
    }

    @Override
    public Status status()
    {
        return Status.Transaction.TransactionAccessedConcurrently;
    }
}
