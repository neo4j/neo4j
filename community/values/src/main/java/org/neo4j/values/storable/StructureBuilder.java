/*
 * Copyright (c) 2002-2018 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
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

import org.neo4j.values.AnyValue;

public abstract class StructureBuilder<Input, Result>
{
    public abstract StructureBuilder<Input,Result> add( String field, Input value );

    public abstract Result build();

    StructureBuilder()
    {
        // Particular subclasses defined in this package
    }

    static long unpackInteger( String name, AnyValue value )
    {
        if ( value == null )
        {
            return 0;
        }
        if ( value instanceof IntegralValue )
        {
            return ((IntegralValue) value).longValue();
        }
        throw new IllegalArgumentException(
                name + " must be an integer value, but was a " + value.getClass().getSimpleName() );
    }
}
