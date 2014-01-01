/**
 * Copyright (c) 2002-2014 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.desktop.config.portable;

import java.io.BufferedReader;
import java.io.File;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintWriter;

import org.neo4j.helpers.Function;

public class Template
{
    private final InputStream templateFile;
    private final VariableSubstitutor substitutor;
    private Function<String, String> substitutionFunction;

    public Template( InputStream templateFile )
    {
        this.templateFile = templateFile;
        substitutor = new VariableSubstitutor();
        substitutionFunction = new Function<String, String>()
        {
            @Override
            public String apply( String name )
            {
                String var = System.getenv( name );
                return var == null ? "" : var;
            }
        };
    }

    public void write( File file ) throws Exception
    {
        try ( BufferedReader reader = new BufferedReader( new InputStreamReader( templateFile, "UTF-8" ) );
              PrintWriter writer = new PrintWriter( file ))
        {
            String input = reader.readLine();
            while ( input != null )
            {
                String output = substitutor.substitute( input, substitutionFunction );

                if ( output == null )
                {
                    continue;
                }

                writer.println( output );
                input = reader.readLine();
            }
        }
    }
}
