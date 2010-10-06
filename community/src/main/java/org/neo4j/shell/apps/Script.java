/**
 * Copyright (c) 2002-2010 "Neo Technology,"
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

package org.neo4j.shell.apps;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;

import org.neo4j.helpers.Service;
import org.neo4j.shell.App;
import org.neo4j.shell.AppCommandParser;
import org.neo4j.shell.OptionDefinition;
import org.neo4j.shell.OptionValueType;
import org.neo4j.shell.Output;
import org.neo4j.shell.Session;
import org.neo4j.shell.impl.AbstractApp;

@Service.Implementation( App.class )
public class Script extends AbstractApp
{
    {
        addOptionDefinition( "v", new OptionDefinition( OptionValueType.NONE,
                "Verbose, print commands" ) );
    }

    @Override
    public String getDescription()
    {
        return "Executes a script of shell commands, supply a file name " +
        		"containing the script";
    }

    public String execute( AppCommandParser parser, Session session,
            Output out ) throws Exception
    {
        boolean verbose = parser.options().containsKey( "v" );
        File file = new File( parser.arguments().get( 0 ) );
        BufferedReader reader = null;
        try
        {
            if ( !file.exists() )
            {
                out.println( "Couldn't find file '" +
                        file.getAbsolutePath() + "'" );
                return null;
            }
            reader = new BufferedReader( new FileReader( file ) );
            String line = null;
            int counter = 0;
            while ( ( line = reader.readLine() ) != null )
            {
                if ( verbose )
                {
                    if ( counter++ > 0 )
                    {
                        out.println();
                    }
                    out.println( "[" + line + "]" );
//                    out.println();
                }
                getServer().interpretLine( line, session, out );
            }
        }
        finally
        {
            safeClose( reader );
        }
        return null;
    }
}
