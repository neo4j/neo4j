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

import org.apache.commons.lang3.text.WordUtils;

import org.neo4j.helpers.Args;

public class OptionalBooleanArg extends OptionalNamedArg
{

    public OptionalBooleanArg( String name, boolean defaultValue, String description )
    {
        super( name, new String[]{"true", "false"}, Boolean.toString( defaultValue ), description );
    }

    @Override
    public String usage()
    {
        return WordUtils.wrap( String.format( "[--%s[=<%s>]]", name, exampleValue ), 60 );
    }

    @Override
    public String parse( Args parsedArgs )
    {
        return Boolean.toString( parsedArgs.getBoolean( name, Boolean.parseBoolean( defaultValue ) ) );
    }
}
