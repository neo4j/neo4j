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

import java.util.function.Function;

import org.neo4j.internal.helpers.Exceptions;
import org.neo4j.internal.kernel.api.procs.DefaultParameterValue;
import org.neo4j.internal.kernel.api.procs.Neo4jTypes;

final class CompositeConverter implements Function<String,DefaultParameterValue>
{
    private final Neo4jTypes.AnyType upCastTo;
    private final Function<String,DefaultParameterValue>[] functions;

    @SafeVarargs
    CompositeConverter( Neo4jTypes.AnyType upCastTo, Function<String,DefaultParameterValue>... functions )
    {
        this.upCastTo = upCastTo;
        this.functions = functions;
    }

    @Override
    public DefaultParameterValue apply( String s )
    {
        IllegalArgumentException lastException = null;
        for ( Function<String,DefaultParameterValue> function : functions )
        {
            try
            {
                return function.apply( s ).castAs( upCastTo );
            }
            catch ( IllegalArgumentException invalidConversion )
            {
                lastException = Exceptions.chain( lastException, invalidConversion );
            }
        }

        // lastException will not be null when we arrive here
        // we either return in the non-exceptional case or we have a valid
        // non-null exception due to the contract of Exceptions#chain
        //noinspection ConstantConditions
        throw lastException;
    }
}
