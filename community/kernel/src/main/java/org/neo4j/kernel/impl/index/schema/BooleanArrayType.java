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

import java.util.Arrays;
import java.util.StringJoiner;

import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.ValueGroup;
import org.neo4j.values.storable.ValueWriter;
import org.neo4j.values.storable.Values;

import static org.neo4j.kernel.impl.index.schema.GenericKey.FALSE;
import static org.neo4j.kernel.impl.index.schema.GenericKey.TRUE;

// <Boolean> as generic raw type is mostly for show, this class overrides default object:y behaviour to create primitive boolean[] array
class BooleanArrayType extends AbstractArrayType<Boolean>
{
    // Affected key state:
    // long0Array

    BooleanArrayType( byte typeId )
    {
        super( ValueGroup.BOOLEAN_ARRAY, typeId, ( o1, o2, i ) -> BooleanType.compare(
                        o1.long0Array[i],
                        o2.long0Array[i] ),
                null,
                ( c, k, i ) -> BooleanType.put( c, k.long0Array[i] ),
                BooleanType::read, null, ValueWriter.ArrayType.BOOLEAN );
    }

    @Override
    int valueSize( GenericKey state )
    {
        return arrayKeySize( state, GenericKey.SIZE_BOOLEAN );
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

    @Override
    Value asValue( GenericKey state )
    {
        boolean[] array = new boolean[state.arrayLength];
        for ( int i = 0; i < state.arrayLength; i++ )
        {
            array[i] = BooleanType.asValueRaw( state.long0Array[i] );
        }
        return Values.of( array );
    }

    void write( GenericKey state, int offset, boolean value )
    {
        state.long0Array[offset] = value ? TRUE : FALSE;
    }

    @Override
    protected void addTypeSpecificDetails( StringJoiner joiner, GenericKey state )
    {
        joiner.add( "long0Array=" + Arrays.toString( state.long0Array ) );
        super.addTypeSpecificDetails( joiner, state );
    }
}
