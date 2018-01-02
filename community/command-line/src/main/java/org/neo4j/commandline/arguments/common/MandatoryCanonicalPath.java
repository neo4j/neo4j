/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.commandline.arguments.common;


import org.neo4j.commandline.Util;
import org.neo4j.commandline.arguments.MandatoryNamedArg;
import org.neo4j.helpers.Args;

public class MandatoryCanonicalPath extends MandatoryNamedArg
{

    public MandatoryCanonicalPath( String name, String exampleValue, String description )
    {
        super( name, exampleValue, description );
    }

    private static String canonicalize( String path )
    {
        if ( path.isEmpty() )
        {
            return path;
        }

        return Util.canonicalPath( path ).toString();
    }

    @Override
    public String parse( Args parsedArgs )
    {
        return canonicalize( super.parse( parsedArgs ) );
    }
}
