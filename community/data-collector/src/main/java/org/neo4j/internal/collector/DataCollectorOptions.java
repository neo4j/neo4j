/*
 * Copyright (c) 2002-2020 "Neo4j,"
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
package org.neo4j.internal.collector;

import java.util.Map;

import org.neo4j.kernel.api.exceptions.InvalidArgumentsException;

/**
 * Helper classes which are used to define options to data collector procedures.
 */
class DataCollectorOptions
{
    private DataCollectorOptions()
    {
    }

    abstract static class Option<T>
    {
        final String name;
        final T defaultValue;

        Option( String name, T defaultValue )
        {
            this.name = name;
            this.defaultValue = defaultValue;
        }

        abstract T parse( Object value ) throws InvalidArgumentsException;

        T parseOrDefault( Map<String,Object> valueMap ) throws InvalidArgumentsException
        {
            if ( valueMap.containsKey( name ) )
            {
                Object o = valueMap.get( name );
                return parse( o );
            }
            return defaultValue;
        }
    }

    static class IntOption extends Option<Integer>
    {
        IntOption( String name, int defaultValue )
        {
            super( name, defaultValue );
        }

        @Override
        Integer parse( Object value ) throws InvalidArgumentsException
        {
            int x = asInteger( value );
            if ( x < 0 )
            {
                throw new InvalidArgumentsException(
                        String.format( "Option `%s` requires positive integer argument, got `%d`", name, x ) );
            }
            return x;
        }

        private int asInteger( Object value ) throws InvalidArgumentsException
        {
            if ( value instanceof Byte ||
                 value instanceof Short ||
                 value instanceof Integer ||
                 value instanceof Long )
            {
                return ((Number)value).intValue();
            }
            throw new InvalidArgumentsException(
                    String.format( "Option `%s` requires integer argument, got `%s`", name, value ) );
        }
    }
}
