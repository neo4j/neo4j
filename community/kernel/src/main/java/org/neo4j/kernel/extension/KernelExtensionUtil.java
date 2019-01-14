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
package org.neo4j.kernel.extension;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;

import static java.io.File.pathSeparator;
import static java.lang.System.getProperty;

/**
 * Can distill information about available kernel extensions into a string, suitable for inclusion
 * in an exception.
 */
public abstract class KernelExtensionUtil
{
    private KernelExtensionUtil()
    {
        throw new AssertionError(); // no instances
    }

    public static String servicesClassPathEntryInformation()
    {
        String separator = System.lineSeparator();
        StringBuilder result = new StringBuilder( "Kernel extensions available on classpath: " );
        StringBuilder classPath = new StringBuilder();
        for ( String entry : getProperty( "java.class.path" ).split( pathSeparator ) )
        {
            classPath.append( separator ).append( "  " ).append( entry );

            File entryFile = new File( entry );
            if ( entryFile.isDirectory() )
            {   // Might we have a directory containing META-INF/services here?
                File servicesDir = new File( new File( entryFile, "META-INF" ), "services" );
                if ( servicesDir.exists() )
                {
                    result.append( separator )
                            .append( "Listing service files and kernel extensions where possible in " )
                            .append( servicesDir ).append( ':' );
                    File[] files = servicesDir.listFiles();
                    if ( files != null )
                    {
                        for ( File serviceFile : files )
                        {
                            if ( serviceFile.isFile() )
                            {
                                result.append( separator ).append( "  " ).append( serviceFile.getName() );
                            }
                        }
                    }

                    File extensionsFile = new File( servicesDir, KernelExtensionFactory.class.getName() );
                    if ( extensionsFile.exists() )
                    {
                        appendKernelExtensionsList( extensionsFile, result, separator + "   + " );
                    }
                }
            }
        }
        return result.append( separator ).append( separator )
                .append( "Class path entries:" ).append( classPath ).toString();
    }

    private static void appendKernelExtensionsList( File file, StringBuilder to, String separator )
    {
        try ( BufferedReader reader = new BufferedReader( new FileReader( file ) ) )
        {
            String line;
            while ( (line = reader.readLine()) != null )
            {
                boolean exists = tryLoadClass( line );
                to.append( separator ).append( line ).append( " (" ).append( exists ? "exists" : "DOES NOT exist" ).append( ')' );
            }
        }
        catch ( IOException e )
        {
            to.append( "Couldn't read due to " ).append( e.getMessage() );
        }
    }

    private static boolean tryLoadClass( String className )
    {
        try
        {
            Class.forName( className );
            return true;
        }
        catch ( ClassNotFoundException e )
        {
            return false;
        }
    }
}
