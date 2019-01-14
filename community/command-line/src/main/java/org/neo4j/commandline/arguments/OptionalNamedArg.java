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
package org.neo4j.commandline.arguments;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import org.neo4j.helpers.Args;

import static org.neo4j.kernel.impl.util.Converters.identity;
import static org.neo4j.kernel.impl.util.Converters.withDefault;

public class OptionalNamedArg implements NamedArgument
{
    protected final String name;
    protected final String exampleValue;
    protected final String defaultValue;
    protected final String description;
    protected final String[] allowedValues;

    public OptionalNamedArg( String name, String exampleValue, String defaultValue, String description )
    {
        this.name = name;
        this.exampleValue = exampleValue;
        this.defaultValue = defaultValue;
        this.description = description;
        allowedValues = new String[]{};
    }

    public OptionalNamedArg( String name, String[] allowedValues, String defaultValue, String description )
    {
        this.name = name;
        this.allowedValues = allowedValues;
        this.exampleValue = String.join( "|", allowedValues );
        this.defaultValue = defaultValue;
        this.description = description;
    }

    @Override
    public String optionsListing()
    {
        return String.format( "--%s=<%s>", name, exampleValue );
    }

    @Override
    public String usage()
    {
        return String.format( "[--%s=<%s>]", name, exampleValue );
    }

    @Override
    public String description()
    {
        return description;
    }

    @Override
    public String name()
    {
        return name;
    }

    @Override
    public String exampleValue()
    {
        return exampleValue;
    }

    public String defaultValue()
    {
        return defaultValue;
    }

    @Override
    public String parse( Args parsedArgs )
    {
        String value = parsedArgs.interpretOption( name, withDefault( defaultValue ), identity() );
        if ( allowedValues.length > 0 )
        {
            for ( String allowedValue : allowedValues )
            {
                if ( allowedValue.equals( value ) )
                {
                    return value;
                }
            }
            throw new IllegalArgumentException(
                    String.format( "'%s' must be one of [%s], not: %s", name, String.join( ",", allowedValues ),
                            value ) );
        }
        return value;
    }

    @Override
    public Collection<String> parseMultiple( Args parsedArgs )
    {
        Collection<String> values = parsedArgs.interpretOptions( name, withDefault( defaultValue ), identity() );
        for ( String value : values )
        {
            if ( allowedValues.length > 0 )
            {
                List<String> allowed = Arrays.asList( allowedValues );
                if ( !allowed.contains( value ) )
                {
                    throw new IllegalArgumentException(
                            String.format( "'%s' must be one of [%s], not: %s", name, String.join( ",", allowedValues ),
                                    value ) );
                }
            }
        }
        return values;
    }
}
