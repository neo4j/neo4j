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
package org.neo4j.procedure.impl;

import java.util.Iterator;
import java.util.List;
import java.util.function.Function;

import org.neo4j.internal.kernel.api.procs.DefaultParameterValue;
import org.neo4j.internal.kernel.api.procs.Neo4jTypes;

final class CompositeConverter implements Function<String,DefaultParameterValue>
{
    private final Neo4jTypes.AnyType upCastTo;
    private final Iterable<Function<String,DefaultParameterValue>> functions;

    @SafeVarargs
    CompositeConverter( Neo4jTypes.AnyType upCastTo, Function<String,DefaultParameterValue>... functions )
    {
        this.upCastTo = upCastTo;
        this.functions = List.of( functions );
    }

    @Override
    public DefaultParameterValue apply( String s )
    {
        for ( Iterator<Function<String,DefaultParameterValue>> iterator = functions.iterator(); iterator.hasNext(); )
        {
            Function<String,DefaultParameterValue> function = iterator.next();
            try
            {
                return function.apply( s ).castAs( upCastTo );
            }
            catch ( IllegalArgumentException invalidConversion )
            {
                if ( !iterator.hasNext() )
                {
                    throw invalidConversion;
                }
            }
        }

        throw new IllegalArgumentException( String.format( "%s is not a valid default value expression", s ) );
    }
}
