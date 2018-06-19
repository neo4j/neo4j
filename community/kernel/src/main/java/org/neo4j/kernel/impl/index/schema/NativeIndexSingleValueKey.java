/*
 * Copyright (c) 2002-2018 "Neo4j,"
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

import org.neo4j.values.storable.Value;

import static org.neo4j.helpers.collection.Iterators.array;

/**
 * Includes value and entity id (to be able to handle non-unique values).
 * This is the abstraction of what NativeSchemaIndex with friends need from a schema key.
 * Note that it says nothing about how keys are compared, serialized, read, written, etc. That is the job of Layout.
 */
abstract class NativeIndexSingleValueKey<SELF extends NativeIndexSingleValueKey<SELF>> extends NativeIndexKey<SELF>
{
    @Override
    protected void assertValidValues( Value... values )
    {
        if ( values.length > 1 )
        {
            throw new IllegalArgumentException( "Tried to create composite key with non-composite schema key layout" );
        }
        if ( values.length < 1 )
        {
            throw new IllegalArgumentException( "Tried to create key without value" );
        }
        assertCorrectType( values[0] );
    }

    @Override
    protected void writeValues( Value[] values )
    {
        values[0].writeTo( this );
    }

    abstract Value assertCorrectType( Value value );

    @Override
    protected String propertiesAsString()
    {
        return asValue().toString();
    }

    abstract Value asValue();

    @Override
    Value[] asValues()
    {
        return array( asValue() );
    }
}
