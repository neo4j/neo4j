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
package org.neo4j.values.storable;

import java.util.function.Function;

import org.neo4j.values.StructureBuilder;

public final class InputMappingStructureBuilder<Input, Internal, Result> implements StructureBuilder<Input,Result>
{
    public static <R> StructureBuilder<Object,R> fromValues( StructureBuilder<? super Value,R> builder )
    {
        return mapping( Values::of, builder );
    }

    public static <I, N, O> StructureBuilder<I,O> mapping( Function<I,N> mapping, StructureBuilder<N,O> builder )
    {
        return new InputMappingStructureBuilder<>( mapping, builder );
    }

    private final Function<Input,Internal> mapping;
    private final StructureBuilder<Internal,Result> builder;

    private InputMappingStructureBuilder( Function<Input,Internal> mapping, StructureBuilder<Internal,Result> builder )
    {
        this.mapping = mapping;
        this.builder = builder;
    }

    @Override
    public StructureBuilder<Input,Result> add( String field, Input value )
    {
        builder.add( field, mapping.apply( value ) );
        return this;
    }

    @Override
    public Result build()
    {
        return builder.build();
    }
}
