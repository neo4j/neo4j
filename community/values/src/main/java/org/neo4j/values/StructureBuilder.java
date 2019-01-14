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
package org.neo4j.values;

import java.util.Map;

import org.neo4j.values.utils.InvalidValuesArgumentException;
import org.neo4j.values.virtual.MapValue;

public interface StructureBuilder<Input, Result>
{
    StructureBuilder<Input,Result> add( String field, Input value );

    Result build();

    static <T> T build( StructureBuilder<AnyValue,T> builder, MapValue map )
    {
        if ( map.size() == 0 )
        {
            throw new InvalidValuesArgumentException( "At least one temporal unit must be specified." );
        }
        return build( builder, map.entrySet() );
    }

    static <T> T build( StructureBuilder<AnyValue,T> builder, Iterable<Map.Entry<String,AnyValue>> entries )
    {
        for ( Map.Entry<String,AnyValue> entry : entries )
        {
            builder = builder.add( entry.getKey(), entry.getValue() );
        }
        return builder.build();
    }
}
