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

import org.neo4j.helpers.Args;

import static org.neo4j.kernel.impl.util.Converters.identity;
import static org.neo4j.kernel.impl.util.Converters.mandatory;

public class MandatoryNamedArg implements NamedArgument
{
    private final String name;
    private final String exampleValue;
    private final String description;

    public MandatoryNamedArg( String name, String exampleValue, String description )
    {
        this.name = name;
        this.exampleValue = exampleValue;
        this.description = description;
    }

    @Override
    public String optionsListing()
    {
        return usage();
    }

    @Override
    public String usage()
    {
        return String.format( "--%s=<%s>", name, exampleValue );
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

    @Override
    public String parse( Args parsedArgs )
    {
        return parsedArgs.interpretOption( name, mandatory(), identity() );
    }

    @Override
    public Collection<String> parseMultiple( Args parsedArgs )
    {
        return parsedArgs.interpretOptions( name, mandatory(), identity() );
    }
}
