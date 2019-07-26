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
package org.neo4j.internal.helpers;

import java.util.Arrays;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.neo4j.util.Preconditions;

public class EnumNameLookup
{
    private EnumNameLookup()
    {
    }

    private static <E extends Enum<E>> Map<String,E> lookupTable( Class<E> cls )
    {
        Preconditions.checkArgument( cls.isEnum(), cls + " is not an Enum type." );
        E[] constants = cls.getEnumConstants();
        return Arrays.stream( constants ).collect( Collectors.toUnmodifiableMap( Enum::name, Function.identity() ) );
    }

    public static <E extends Enum<E>> Function<String,E> fromString( Class<E> cls )
    {
        Map<String,E> table = lookupTable( cls );
        return name ->
        {
            E constant = table.get( name );
            if ( constant == null )
            {
                throw new IllegalArgumentException( "'" + name + "' is not a variant of the " + cls.getSimpleName() + " enum." );
            }
            return constant;
        };
    }
}
