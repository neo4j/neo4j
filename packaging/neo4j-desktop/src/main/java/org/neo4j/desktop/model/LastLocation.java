/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
package org.neo4j.desktop.model;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.Scanner;

public class LastLocation
{
    public static String getLastLocation( String defaultLocation )
    {
        File file = new File( ".dblocation" );
        String location = defaultLocation;

        if ( file.exists() && file.canRead() )
        {
            try ( Scanner scanner = new Scanner( file ) )
            {
                if ( scanner.hasNextLine() )
                {
                    location = scanner.nextLine();
                }
            }
            catch ( FileNotFoundException fnfe )
            {
                fnfe.printStackTrace();
            }
        }

        return location;
    }

    public static String setLastLocation( String location )
    {
        try
        {
            java.nio.file.Files.write( Paths.get( ".dblocation" ),location.getBytes() );
        }
        catch ( IOException ioe )
        {
            System.out.println( "Error saving DB location" );
            System.out.println( ioe );
        }

        return location;
    }
}
