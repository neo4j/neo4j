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

import java.time.ZonedDateTime;
import java.util.Arrays;
import java.util.StringJoiner;

import org.neo4j.values.storable.ValueGroup;
import org.neo4j.values.storable.ValueWriter;

class ZonedDateTimeArrayType extends AbstractArrayType<ZonedDateTime>
{
    // Affected key state:
    // long0Array (epochSecondUTC)
    // long1Array (nanoOfSecond)
    // long2Array (zoneId)
    // long3Array (zoneOffsetSeconds)

    ZonedDateTimeArrayType( byte typeId )
    {
        super( ValueGroup.ZONED_DATE_TIME_ARRAY, typeId, ( o1, o2, i ) -> ZonedDateTimeType.compare(
                        // intentional long1 and long2 - not the array versions
                        o1.long0Array[i], o1.long1Array[i], o1.long2Array[i], o1.long3Array[i],
                        o2.long0Array[i], o2.long1Array[i], o2.long2Array[i], o2.long3Array[i] ),
                ( k, i ) -> ZonedDateTimeType.asValueRaw( k.long0Array[i], k.long1Array[i], k.long2Array[i], k.long3Array[i] ),
                ( c, k, i ) -> ZonedDateTimeType.put( c, k.long0Array[i], k.long1Array[i], k.long2Array[i], k.long3Array[i] ),
                ZonedDateTimeType::read, ZonedDateTime[]::new, ValueWriter.ArrayType.ZONED_DATE_TIME );
    }

    @Override
    int valueSize( GenericKey state )
    {
        return arrayKeySize( state, GenericKey.SIZE_ZONED_DATE_TIME );
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

    void initializeArray( GenericKey key, int length, ValueWriter.ArrayType arrayType )
    {
        key.long0Array = ensureBigEnough( key.long0Array, length );
        key.long1Array = ensureBigEnough( key.long1Array, length );
        key.long2Array = ensureBigEnough( key.long2Array, length );
        key.long3Array = ensureBigEnough( key.long3Array, length );
    }

    void write( GenericKey state, int offset, long epochSecondUTC, int nano, short zoneId, int offsetSeconds )
    {
        state.long0Array[offset] = epochSecondUTC;
        state.long1Array[offset] = nano;
        state.long2Array[offset] = zoneId;
        state.long3Array[offset] = offsetSeconds;
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
