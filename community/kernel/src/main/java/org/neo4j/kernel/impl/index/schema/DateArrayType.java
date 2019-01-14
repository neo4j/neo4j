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
package org.neo4j.kernel.impl.index.schema;

import java.time.LocalDate;
import java.util.Arrays;
import java.util.StringJoiner;

import org.neo4j.values.storable.ValueGroup;
import org.neo4j.values.storable.ValueWriter;

class DateArrayType extends AbstractArrayType<LocalDate>
{
    // Affected key state:
    // long0Array (epochDay)

    DateArrayType( byte typeId )
    {
        super( ValueGroup.DATE_ARRAY, typeId, ( o1, o2, i ) -> DateType.compare(
                        o1.long0Array[i],
                        o2.long0Array[i] ),
                ( k, i ) -> DateType.asValueRaw( k.long0Array[i] ),
                ( c, k, i ) -> DateType.put( c, k.long0Array[i] ),
                DateType::read, LocalDate[]::new, ValueWriter.ArrayType.DATE );
    }

    @Override
    int valueSize( GenericKey state )
    {
        return arrayKeySize( state, GenericKey.SIZE_DATE );
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

    public void write( GenericKey state, int offset, long epochDay )
    {
        state.long0Array[offset] = epochDay;
    }

    @Override
    protected void addTypeSpecificDetails( StringJoiner joiner, GenericKey state )
    {
        joiner.add( "long0Array=" + Arrays.toString( state.long0Array ) );
        super.addTypeSpecificDetails( joiner, state );
    }
}
