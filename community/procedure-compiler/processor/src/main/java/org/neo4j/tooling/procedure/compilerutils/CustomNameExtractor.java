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
package org.neo4j.tooling.procedure.compilerutils;

import java.util.Optional;
import java.util.function.Supplier;

import org.neo4j.procedure.Procedure;
import org.neo4j.procedure.UserFunction;
import org.neo4j.procedure.UserAggregationFunction;

public class CustomNameExtractor
{
    private CustomNameExtractor()
    {

    }

    /**
     * Extracts user-defined names, usually from a {@link Procedure}, {@link UserFunction}
     * or {@link UserAggregationFunction}.
     *
     * As such, extracted strings are assumed to be non-null.
     */
    public static Optional<String> getName( Supplier<String> nameSupplier, Supplier<String> valueSupplier )
    {
        String name = nameSupplier.get().trim();
        if ( !name.isEmpty() )
        {
            return Optional.of( name );
        }
        String value = valueSupplier.get().trim();
        if ( !value.isEmpty() )
        {
            return Optional.of( value );
        }
        return Optional.empty();
    }
}
