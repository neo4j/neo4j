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

import java.util.Collection;
import java.util.stream.Collectors;

import org.neo4j.helpers.Args;

import static org.neo4j.kernel.impl.util.Converters.identity;
import static org.neo4j.kernel.impl.util.Converters.withDefault;

/**
 * Some arguments can have a variable name, such as in neo4j-admin import where the `--relationships` argument may
 * have the following form:
 *
 * --relationships:MYTYPE=file.csv
 *
 * or the `--nodes` argument which can be
 *
 * --nodes:TYPE1:TYPE2=file.csv
 *
 * This is only used for validation, not to actually read the metadata. See ImportCommand.java.
 */
public class OptionalNamedArgWithMetadata extends OptionalNamedArg implements NamedArgument
{
    protected final String exampleMetaData;

    public OptionalNamedArgWithMetadata( String name, String exampleMetaData, String exampleValue, String defaultValue,
            String description )
    {
        super( name, exampleValue, defaultValue, description );
        this.exampleMetaData = exampleMetaData;
    }

    @Override
    public String optionsListing()
    {
        return String.format( "--%s[%s]=<%s>", name, exampleMetaData, exampleValue );
    }

    @Override
    public String usage()
    {
        return String.format( "[--%s[%s]=<%s>]", name, exampleMetaData, exampleValue );
    }

    @Override
    public String parse( Args parsedArgs )
    {
        throw new RuntimeException( "Arguments with metadata only support multiple value parsing" );
    }

    @Override
    public Collection<String> parseMultiple( Args parsedArgs )
    {
        Collection<Args.Option<String>> vals =
                parsedArgs.interpretOptionsWithMetadata( name, withDefault( defaultValue ), identity() );

        return vals.stream().map( Args.Option::value ).collect( Collectors.toList() );
    }
}

