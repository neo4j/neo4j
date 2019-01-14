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

import org.neo4j.util.Preconditions;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.ValueGroup;

import static org.neo4j.helpers.collection.Iterators.array;

/**
 * Includes value and entity id (to be able to handle non-unique values).
 * This is the abstraction of what NativeSchemaIndex with friends need from a schema key.
 * Note that it says nothing about how keys are compared, serialized, read, written, etc. That is the job of Layout.
 *
 * // TODO some of the methods in here are here to mimic the old interface and are kept/reinstated simply to reduce changes in current code
 */
abstract class NativeIndexSingleValueKey<SELF extends NativeIndexSingleValueKey<SELF>> extends NativeIndexKey<SELF>
{
    @Override
    void assertValidValue( int stateSlot, Value value )
    {
        //noinspection ResultOfMethodCallIgnored
        Preconditions.requireExactlyZero( stateSlot );
        assertCorrectType( value );
    }

    @Override
    void writeValue( int stateSlot, Value value, Inclusion inclusion )
    {
        value.writeTo( this );
    }

    @Override
    final void initValueAsLowest( int stateSlot, ValueGroup valueGroup )
    {
        initValueAsLowest( valueGroup );
    }

    abstract void initValueAsLowest( ValueGroup valueGroups );

    @Override
    final void initValueAsHighest( int stateSlot, ValueGroup valueGroup )
    {
        initValueAsHighest( valueGroup );
    }

    abstract void initValueAsHighest( ValueGroup valueGroups );

    abstract Value assertCorrectType( Value value );

    abstract Value asValue();

    @Override
    Value[] asValues()
    {
        return array( asValue() );
    }

    void from( Value value )
    {
        assertCorrectType( value );
        value.writeTo( this );
    }

    @Override
    int numberOfStateSlots()
    {
        return 1;
    }
}
