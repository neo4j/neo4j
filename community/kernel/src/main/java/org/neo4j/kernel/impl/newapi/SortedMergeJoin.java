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
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.Values;

import static org.neo4j.util.Preconditions.checkArgument;

/**
 * A sort merge join that sorts entities by their values (properties).
 */
final class SortedMergeJoin
{
    private static final int NO_ENTITY = -1;

    private long nextFromA = NO_ENTITY;
    private long nextFromB = NO_ENTITY;
    private Value[] valuesFromA;
    private Value[] valuesFromB;
    private int indexOrder;

    void initialize( IndexOrder indexOrder )
    {
        this.indexOrder = indexOrder == IndexOrder.DESCENDING ? 1 : NO_ENTITY;
        this.nextFromA = NO_ENTITY;
        this.nextFromB = NO_ENTITY;
        this.valuesFromA = null;
        this.valuesFromB = null;
    }

    boolean needsA()
    {
        return nextFromA == NO_ENTITY;
    }

    boolean needsB()
    {
        return nextFromB == NO_ENTITY;
    }

    void setA( long entityId, Value[] values )
    {
        nextFromA = entityId;
        valuesFromA = values;
    }

    void setB( long entityId, Value[] values )
    {
        nextFromB = entityId;
        valuesFromB = values;
    }

    /**
     * Produces a next entity unless it is at the end of the entity stream, in which case it returns {@code false}.
     */
    boolean next( Sink sink )
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

        if ( nextFromB == NO_ENTITY || Integer.signum( c ) == indexOrder )
        {
            if ( nextFromA == NO_ENTITY )
            {
                return false;
            }

            sink.acceptSortedMergeJoin( nextFromA, valuesFromA );
            nextFromA = NO_ENTITY;
            valuesFromA = null;
        }
        else
        {
            sink.acceptSortedMergeJoin( nextFromB, valuesFromB );
            nextFromB = NO_ENTITY;
            valuesFromB = null;
        }

        return true;
    }

    interface Sink
    {
        void acceptSortedMergeJoin( long entityId, Value[] values );
    }
}
