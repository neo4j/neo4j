/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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
package org.neo4j.commandline.arguments;

import org.neo4j.helpers.Args;

import static org.neo4j.kernel.impl.util.Converters.identity;
import static org.neo4j.kernel.impl.util.Converters.withDefault;

public class OptionalBooleanArg extends OptionalNamedArg
{

    public OptionalBooleanArg( String name, boolean defaultValue, String description )
    {
        super( name, new String[]{"true", "false"}, Boolean.toString( defaultValue ), description );
    }

    @Override
    public String parse( String... args )
    {
        String value = Args.parse( args ).interpretOption( name, withDefault( defaultValue ), identity() );

        if ( value == null || value.isEmpty() )
        {
            return Boolean.toString( true );
        }

        if ( allowedValues.length > 0 )
        {
            for ( String allowedValue : allowedValues )
            {
                if ( allowedValue.equalsIgnoreCase( value ) )
                {
                    return value;
                }
            }
            throw new IllegalArgumentException( String.format( "'%s' must be one of [%s], not: %s", name,
                    String.join( ",", allowedValues ), value ) );
        }
        return value;
    }
}
