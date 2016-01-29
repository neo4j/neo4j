package org.neo4j.desktop.model;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.Scanner;

public class LastLocation
{
    public static String getLastLocation( String defaultLocation )
    {
        File file = new File( ".dblocation" );
        String location = defaultLocation;

        if( file.exists() && file.canRead() )
        {
            try( Scanner scanner = new Scanner( file ) )
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
}
