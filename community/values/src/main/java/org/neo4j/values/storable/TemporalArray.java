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
package org.neo4j.values.storable;

import java.time.temporal.Temporal;

import org.neo4j.graphdb.spatial.Geometry;
import org.neo4j.values.AnyValue;

public abstract class TemporalArray<T extends Temporal & Comparable<? super T>, V extends TemporalValue<T,V>> extends NonPrimitiveArray<T>
{

    @Override
    public boolean equals( Geometry[] x )
    {
        return false;
    }

    protected final <E extends Exception> void writeTo( ValueWriter<E> writer, ValueWriter.ArrayType type, Temporal[] values ) throws E
    {
        writer.beginArray( values.length, type );
        for ( Temporal x : values )
        {
            Value value = Values.temporalValue( x );
            value.writeTo( writer );
        }
        writer.endArray();
    }

    @Override
    public final AnyValue value( int offset )
    {
        return Values.temporalValue( value()[offset] );
    }
}
