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
package org.neo4j.kernel.impl.index.schema;

import java.util.Arrays;
import java.util.StringJoiner;

import org.neo4j.values.storable.DurationValue;
import org.neo4j.values.storable.ValueGroup;
import org.neo4j.values.storable.ValueWriter;

// Raw type is DurationValue, but that's because DurationValue doesn't wrap a TemporalValue or derivative, rather extracts fields from it
class DurationArrayType extends AbstractArrayType<DurationValue>
{
    // Affected key state:
    // long0Array (totalAvgSeconds)
    // long1Array (nanosOfSecond)
    // long2Array (months)
    // long3Array (days)

    DurationArrayType( byte typeId )
    {
        super( ValueGroup.DURATION_ARRAY, typeId, ( o1, o2, i ) -> DurationType.compare(
                        o1.long0Array[i], o1.long1Array[i], o1.long2Array[i], o1.long3Array[i],
                        o2.long0Array[i], o2.long1Array[i], o2.long2Array[i], o2.long3Array[i] ),
                ( k, i ) -> DurationType.asValue( k.long0Array[i], k.long1Array[i], k.long2Array[i], k.long3Array[i] ),
                ( c, k, i ) -> DurationType.put( c, k.long0Array[i], k.long1Array[i], k.long2Array[i], k.long3Array[i] ),
                DurationType::read, DurationValue[]::new, ValueWriter.ArrayType.DURATION );
    }

    @Override
    int valueSize( GenericKey state )
    {
        return arrayKeySize( state, GenericKey.SIZE_DURATION );
    }

    @Override
    void copyValue( GenericKey to, GenericKey from, int length )
    {
        initializeArray( to, length, null );
        System.arraycopy( from.long0Array, 0, to.long0Array, 0, length );
        System.arraycopy( from.long1Array, 0, to.long1Array, 0, length );
        System.arraycopy( from.long2Array, 0, to.long2Array, 0, length );
        System.arraycopy( from.long3Array, 0, to.long3Array, 0, length );
    }

    @Override
    void initializeArray( GenericKey key, int length, ValueWriter.ArrayType arrayType )
    {
        key.long0Array = ensureBigEnough( key.long0Array, length );
        key.long1Array = ensureBigEnough( key.long1Array, length );
        key.long2Array = ensureBigEnough( key.long2Array, length );
        key.long3Array = ensureBigEnough( key.long3Array, length );
    }

    void write( GenericKey state, int offset, long months, long days, long totalAvgSeconds, int nanos )
    {
        state.long0Array[offset] = totalAvgSeconds;
        state.long1Array[offset] = nanos;
        state.long2Array[offset] = months;
        state.long3Array[offset] = days;
    }

    @Override
    protected void addTypeSpecificDetails( StringJoiner joiner, GenericKey state )
    {
        joiner.add( "long0Array=" + Arrays.toString( state.long0Array ) );
        joiner.add( "long1Array=" + Arrays.toString( state.long1Array ) );
        joiner.add( "long2Array=" + Arrays.toString( state.long2Array ) );
        joiner.add( "long3Array=" + Arrays.toString( state.long3Array ) );
        super.addTypeSpecificDetails( joiner, state );
    }
}
