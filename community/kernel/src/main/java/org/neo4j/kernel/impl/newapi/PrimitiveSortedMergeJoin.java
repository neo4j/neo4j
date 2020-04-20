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
package org.neo4j.kernel.impl.newapi;

import org.neo4j.internal.schema.IndexOrder;

import static java.lang.Long.signum;
import static java.lang.Math.subtractExact;

/**
 * A sort merge join that sorts nodes by their ids
 */
final class PrimitiveSortedMergeJoin
{
    private static final int NOT_INITIALIZED = -1;
    private long nextFromA = NOT_INITIALIZED;
    private long nextFromB = NOT_INITIALIZED;
    private int indexOrder;

    void initialize( IndexOrder indexOrder )
    {
        this.indexOrder = indexOrder == IndexOrder.DESCENDING ? 1 : -1;
        this.nextFromA = NOT_INITIALIZED;
        this.nextFromB = NOT_INITIALIZED;
    }

    boolean needsA()
    {
        return nextFromA == NOT_INITIALIZED;
    }

    boolean needsB()
    {
        return nextFromB == NOT_INITIALIZED;
    }

    void setA( long nodeId )
    {
        nextFromA = nodeId;
    }

    void setB( long nodeId )
    {
        nextFromB = nodeId;
    }

    long next()
    {
        long difference = 0;
        long result;

        if ( nextFromA != NOT_INITIALIZED && nextFromB != NOT_INITIALIZED )
        {
            difference = subtractExact( nextFromA, nextFromB );
        }

        if ( nextFromB == NOT_INITIALIZED || signum( difference ) == indexOrder )
        {
            result = nextFromA;
            nextFromA = NOT_INITIALIZED;
        }
        else
        {
            result = nextFromB;
            nextFromB = NOT_INITIALIZED;
        }
        return result;
    }
}
