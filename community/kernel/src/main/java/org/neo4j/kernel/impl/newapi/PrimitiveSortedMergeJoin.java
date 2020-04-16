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

/**
 * A sort merge join that sorts nodes by their ids
 */
final class PrimitiveSortedMergeJoin
{
    private long nextFromA = -1;
    private long nextFromB = -1;
    private int indexOrder;

    void initialize( IndexOrder indexOrder )
    {
        this.indexOrder = indexOrder == IndexOrder.DESCENDING ? 1 : -1;
        this.nextFromA = -1;
        this.nextFromB = -1;
    }

    boolean needsA()
    {
        return nextFromA == -1;
    }

    boolean needsB()
    {
        return nextFromB == -1;
    }

    void setA( long nodeId )
    {
        nextFromA = nodeId;
    }

    void setB( long nodeId )
    {
        nextFromB = nodeId;
    }

    void next( Sink sink )
    {
        long c = 0;
        if ( nextFromA != -1 && nextFromB != -1 )
        {
            c = nextFromA - nextFromB;
        }

        if ( nextFromB == -1 || Long.signum( c ) == indexOrder )
        {
            sink.acceptSortedMergeJoin( nextFromA );
            nextFromA = -1;
        }
        else
        {
            sink.acceptSortedMergeJoin( nextFromB );
            nextFromB = -1;
        }
    }

    interface Sink
    {
        void acceptSortedMergeJoin( long nodeId );
    }
}
