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
package org.neo4j.kernel.impl.newapi;

import org.neo4j.internal.kernel.api.IndexOrder;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.Values;

import static org.neo4j.util.Preconditions.checkArgument;

final class SortedMergeJoin
{
    private long nextFromA = -1;
    private long nextFromB = -1;
    private Value[] valuesFromA;
    private Value[] valuesFromB;
    private int indexOrder;

    void initialize( IndexOrder indexOrder )
    {
        this.indexOrder = indexOrder == IndexOrder.DESCENDING ? 1 : -1;
        this.nextFromA = -1;
        this.nextFromB = -1;
        this.valuesFromA = null;
        this.valuesFromB = null;
    }

    boolean needsA()
    {
        return nextFromA == -1;
    }

    boolean needsB()
    {
        return nextFromB == -1;
    }

    void setA( long nodeId, Value[] values )
    {
        nextFromA = nodeId;
        valuesFromA = values;
    }

    void setB( long nodeId, Value[] values )
    {
        nextFromB = nodeId;
        valuesFromB = values;
    }

    void next( Sink sink )
    {
        int c = 0;
        if ( valuesFromA != null && valuesFromB != null )
        {
            checkArgument( valuesFromA.length == valuesFromB.length,
                           "Expected index and txState values to have same dimensions, but got %d values from index and %d from txState",
                           valuesFromB.length, valuesFromA.length );

            for ( int i = 0; c == 0 && i < valuesFromA.length; i++ )
            {
                c = Values.COMPARATOR.compare( valuesFromA[i], valuesFromB[i] );
            }
        }

        if ( nextFromB == -1 || Integer.signum( c ) == indexOrder )
        {
            sink.acceptSortedMergeJoin( nextFromA, valuesFromA );
            nextFromA = -1;
            valuesFromA = null;
        }
        else
        {
            sink.acceptSortedMergeJoin( nextFromB, valuesFromB );
            nextFromB = -1;
            valuesFromB = null;
        }
    }

    interface Sink
    {
        void acceptSortedMergeJoin( long nodeId, Value[] values );
    }
}
