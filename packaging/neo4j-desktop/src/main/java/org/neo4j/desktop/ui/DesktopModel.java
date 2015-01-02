/**
 * Copyright (c) 2002-2015 "Neo Technology,"
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
package org.neo4j.desktop.ui;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.PrintWriter;

import org.neo4j.desktop.config.Environment;
import org.neo4j.desktop.config.OperatingSystemFamily;
import org.neo4j.helpers.Function;
import org.neo4j.kernel.Version;

import static java.lang.String.format;
import static org.neo4j.desktop.config.DatabaseConfiguration.copyDefaultDatabaseConfigurationProperties;

public class DesktopModel
{
    private final Environment environment;
    private File databaseDirectory;

    public DesktopModel( Environment environment, File databaseDirectory )
    {
        this.environment = environment;
        this.databaseDirectory = databaseDirectory;
    }

    public String getNeo4jVersion()
    {
        return format( "%s", Version.getKernel().getReleaseVersion() );
    }

    public File getDatabaseDirectory()
    {
        return databaseDirectory;
    }

    public void setDatabaseDirectory( File databaseDirectory ) throws UnsuitableGraphDatabaseDirectory
    {
        verifyGraphDirectory( databaseDirectory );
        this.databaseDirectory = databaseDirectory;
    }


    public File getVmOptionsFile()
    {
        return getUserVmOptionsFile();
    }

    private File getUserVmOptionsFile()
    {
        // The installer creates a .loc file that contains the path of the actual per-user vmoptions file
        String location = getUserVmOptionsFileLocation();
        File vmOptionsFile;

        if ( null == location )
        {
            vmOptionsFile = getDefaultUserVmOptionsFile();
        }
        else
        {
            vmOptionsFile = new File( substituteVars( location ) );
        }

        if ( null != vmOptionsFile &&  !vmOptionsFile.exists() )
        {
            createUserVmOptionsFile( vmOptionsFile );
        }

        return vmOptionsFile;
    }


    private void createUserVmOptionsFile( File vmOptionsFile )
    {
        try {
            FileWriter writer = new FileWriter( vmOptionsFile );
            PrintWriter printer = new PrintWriter( writer );
            try
            {
                FileReader reader = new FileReader( getSystemVmOptionsFile() );
                BufferedReader buffered = new BufferedReader( reader );
                try
                {
                    String line;
                    while ( ( line = buffered.readLine() ) != null )
                    {
                        String trimmed = line.trim();
                        if ( trimmed.startsWith( "#" ) || ! trimmed.startsWith( "-include-options" ) )
                        {
                            printer.println( line );
                        }
                    }
                }
                finally
                {
                    buffered.close();
                }
            }
            finally
            {
                printer.close();
            }
        }
        catch ( IOException e )
        {
            // ignore
            e.printStackTrace( System.out );
        }
    }

    private String getUserVmOptionsFileLocation()
    {
        return readOneLine( new File( environment.getBaseDirectory(), "neo4j-community-user-vmoptions.loc" ) );
    }

    private File getDefaultUserVmOptionsFile()
    {
        if ( OperatingSystemFamily.WINDOWS.isDetected() )
        {
            return new File( new File ( System.getenv( "APPDDATA" ) ), "neo4j-community.vmoptions" );
        }
        else
        {
            return new File( new File ( System.getProperty( "user.home" ) ) , ".neo4j-community.vmoptions" );
        }
    }

    private File getSystemVmOptionsFile()
    {
        return new File( environment.getBaseDirectory(), "neo4j-community.vmoptions" );
    }

    private String substituteVars( String location )
    {
        return new VariableSubstitutor().substitute( location, new Function<String, String>()
        {
            @Override
            public String apply( String name )
            {
                String value = System.getenv( name );
                return value == null ? "" : value;
            }
        } );
    }

    public File getDatabaseConfigurationFile()
    {
        return new File( databaseDirectory, "neo4j.properties" );
    }

    @SuppressWarnings("ResultOfMethodCallIgnored")
    public void prepareGraphDirectoryForStart() throws UnsuitableGraphDatabaseDirectory
    {
        verifyGraphDirectory( databaseDirectory );
        if ( !databaseDirectory.exists() )
        {
            databaseDirectory.mkdirs();
        }

        File configurationFile = getDatabaseConfigurationFile();
        if ( !configurationFile.exists() )
        {
            try
            {
                copyDefaultDatabaseConfigurationProperties( configurationFile );
            }
            catch ( IOException e )
            {
                throw new UnsuitableGraphDatabaseDirectory( "Unable to write default configuration to %s",
                        databaseDirectory );
            }
        }
    }

    private static void verifyGraphDirectory( File dir ) throws UnsuitableGraphDatabaseDirectory
    {
        if ( !dir.isDirectory() )
        {
            throw new UnsuitableGraphDatabaseDirectory( "%s is not a directory", dir );
        }

        if ( !dir.canWrite() )
        {
            throw new UnsuitableGraphDatabaseDirectory( "%s is not writeable", dir );
        }

        String[] fileNames = dir.list( new FilenameFilter()
        {
            @Override public boolean accept( File dir, String name )
            {
                return ! name.startsWith( "." );
            }
        } );

        if ( 0 == fileNames.length )
        {
            return;
        }

        for ( String fileName : fileNames )
        {
            if ( fileName.startsWith( "neostore" ) || fileName.equals( "neo4j.properties" ) )
            {
                return;
            }
        }

        throw new UnsuitableGraphDatabaseDirectory(
                "%s is neither empty nor does it contain a neo4j graph database", dir );
    }

    private static String readOneLine( File file )
    {
        try
        {
            if ( file.exists() && file.isFile() && file.canRead() )
            {
                FileReader reader = new FileReader( file );
                try
                {
                    BufferedReader buffered = new BufferedReader( reader );
                    try
                    {
                        return buffered.readLine();
                    }
                    finally
                    {
                        buffered.close();
                    }

                }
                finally
                {
                    reader.close();
                }
            }
        }
        catch (IOException e)
        {
            // couldn't read file content
            return null;
        }

        // file was not readable or not a file
        return null;
    }
}
