/*
 * Copyright (c) 2002-2018 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
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
package org.neo4j.kernel.api.exceptions.index;

import org.neo4j.kernel.api.exceptions.KernelException;

import static org.neo4j.kernel.api.exceptions.Status.Schema.IndexLimitReached;

/**
 * This exception is thrown when the underlying index implementation is not able to index more entities.
 * It can either be thrown during index population or during insertions at runtime.
 */
public class IndexCapacityExceededException extends KernelException
{
    private static final String RESERVATION_FAILED_MESSAGE =
            "Unable to reserve %d entries for insertion into index. " +
            "Index contains too many entries. Current limitation is %d entries per index. " +
            "Currently there are %d index entities.";

    private static final String INSERTION_FAILED_MESSAGE =
            "Index contains too many entries. Current limitation is %d indexed entities per index. " +
            "Currently there are %d index entities.";

    public IndexCapacityExceededException( long reservation, long currentValue, long limit )
    {
        super( IndexLimitReached, RESERVATION_FAILED_MESSAGE, reservation, currentValue, limit );
    }

    public IndexCapacityExceededException( long currentValue, long limit )
    {
        super( IndexLimitReached, INSERTION_FAILED_MESSAGE, currentValue, limit );
    }
}
