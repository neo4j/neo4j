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

import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.StringJoiner;

import org.neo4j.values.storable.ValueGroup;
import org.neo4j.values.storable.ValueWriter;

class LocalDateTimeArrayType extends AbstractArrayType<LocalDateTime>
{
    // Affected key state:
    // long0Array (nanoOfSecond)
    // long1Array (epochSecond)

    LocalDateTimeArrayType( byte typeId )
    {
        super( ValueGroup.LOCAL_DATE_TIME_ARRAY, typeId, ( o1, o2, i ) -> LocalDateTimeType.compare(
                        o1.long0Array[i], o1.long1Array[i],
                        o2.long0Array[i], o2.long1Array[i] ),
                ( k, i ) -> LocalDateTimeType.asValueRaw( k.long0Array[i], k.long1Array[i] ),
                ( c, k, i ) -> LocalDateTimeType.put( c, k.long0Array[i], k.long1Array[i] ),
                LocalDateTimeType::read, LocalDateTime[]::new, ValueWriter.ArrayType.LOCAL_DATE_TIME );
    }

    @Override
    int valueSize( GenericKey state )
    {
        return arrayKeySize( state, GenericKey.SIZE_LOCAL_DATE_TIME );
    }

    @Override
    void copyValue( GenericKey to, GenericKey from, int length )
    {
        initializeArray( to, length, null );
        System.arraycopy( from.long0Array, 0, to.long0Array, 0, length );
        System.arraycopy( from.long1Array, 0, to.long1Array, 0, length );
    }

    @Override
    void initializeArray( GenericKey key, int length, ValueWriter.ArrayType arrayType )
    {
        key.long0Array = ensureBigEnough( key.long0Array, length );
        key.long1Array = ensureBigEnough( key.long1Array, length );
    }

    void write( GenericKey state, int offset, long epochSecond, int nano )
    {
        state.long0Array[offset] = nano;
        state.long1Array[offset] = epochSecond;
    }

    @Override
    protected void addTypeSpecificDetails( StringJoiner joiner, GenericKey state )
    {
        joiner.add( "long0Array=" + Arrays.toString( state.long0Array ) );
        joiner.add( "long1Array=" + Arrays.toString( state.long1Array ) );
        super.addTypeSpecificDetails( joiner, state );
    }
}
