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

import java.time.LocalTime;
import java.util.Arrays;
import java.util.StringJoiner;

import org.neo4j.values.storable.ValueGroup;
import org.neo4j.values.storable.ValueWriter;

class LocalTimeArrayType extends AbstractArrayType<LocalTime>
{
    // Affected key state:
    // long0Array (nanoOfDay)

    LocalTimeArrayType( byte typeId )
    {
        super( ValueGroup.LOCAL_TIME_ARRAY, typeId, ( o1, o2, i ) -> LocalTimeType.compare(
                        o1.long0Array[i],
                        o2.long0Array[i] ),
                ( k, i ) -> LocalTimeType.asValueRaw( k.long0Array[i] ),
                ( c, k, i ) -> LocalTimeType.put( c, k.long0Array[i] ),
                LocalTimeType::read, LocalTime[]::new, ValueWriter.ArrayType.LOCAL_TIME );
    }

    @Override
    int valueSize( GenericKey state )
    {
        return arrayKeySize( state, GenericKey.SIZE_LOCAL_TIME );
    }

    @Override
    void copyValue( GenericKey to, GenericKey from, int length )
    {
        initializeArray( to, length, null );
        System.arraycopy( from.long0Array, 0, to.long0Array, 0, length );
    }

    @Override
    void initializeArray( GenericKey key, int length, ValueWriter.ArrayType arrayType )
    {
        key.long0Array = ensureBigEnough( key.long0Array, length );
    }

    void write( GenericKey state, int offset, long nanoOfDay )
    {
        state.long0Array[offset] = nanoOfDay;
    }

    @Override
    protected void addTypeSpecificDetails( StringJoiner joiner, GenericKey state )
    {
        joiner.add( "long0Array=" + Arrays.toString( state.long0Array ) );
        super.addTypeSpecificDetails( joiner, state );
    }
}
