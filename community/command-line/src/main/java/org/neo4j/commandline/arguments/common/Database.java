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
package org.neo4j.commandline.arguments.common;


import java.io.File;

import org.neo4j.commandline.arguments.OptionalNamedArg;
import org.neo4j.helpers.Args;

public class Database extends OptionalNamedArg
{
    public static final String ARG_DATABASE = "database";

    public Database()
    {
        this( "Name of database." );
    }

    public Database( String description )
    {
        super( ARG_DATABASE, "name", "graph.db", description );
    }

    private static String validate( String dbName )
    {
        if ( dbName.contains( File.separator ) )
        {
            throw new IllegalArgumentException(
                    "'database' should be a name but you seem to have specified a path: " + dbName );
        }
        return dbName;
    }

    @Override
    public String parse( Args parsedArgs )
    {
        return validate( super.parse( parsedArgs ) );
    }
}
