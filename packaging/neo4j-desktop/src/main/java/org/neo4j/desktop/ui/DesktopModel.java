/*
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
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;

import org.neo4j.desktop.config.Installation;
import org.neo4j.desktop.runtime.DesktopConfigurator;
import org.neo4j.kernel.Version;
import org.neo4j.server.configuration.ConfigurationBuilder;
import static java.lang.String.format;

public class DesktopModel
{
    private final DesktopConfigurator serverConfigurator;
    private final List<DesktopModelListener> listeners = new ArrayList<>();
    private final Installation installation;

    public DesktopModel( Installation installation )
    {
        this.installation = installation;
        this.serverConfigurator = new DesktopConfigurator( installation );

        serverConfigurator.setDatabaseDirectory( installation.getDatabaseDirectory() );
    }

    public ConfigurationBuilder getServerConfigurator() {
        serverConfigurator.refresh();
        for(DesktopModelListener listener : listeners) {
            listener.desktopModelChanged(this);
        }

        return serverConfigurator;
    }

    public String getNeo4jVersion()
    {
        return format( "%s", Version.getKernel().getReleaseVersion() );
    }

    public int getServerPort()
    {
        return serverConfigurator.getServerPort();
    }

    public File getDatabaseDirectory()
    {
        return new File( serverConfigurator.getDatabaseDirectory() );
    }

    public void setDatabaseDirectory( File databaseDirectory ) throws UnsuitableDirectoryException
    {
        verifyGraphDirectory(databaseDirectory);
        serverConfigurator.setDatabaseDirectory( databaseDirectory );
    }


    public File getVmOptionsFile()
    {
        return installation.getVmOptionsFile();
    }

    public File getDatabaseConfigurationFile()
    {
        return serverConfigurator.getDatabaseConfigurationFile();
    }

    public File getServerConfigurationFile()
    {
        return installation.getServerConfigurationsFile();
    }

    @SuppressWarnings("ResultOfMethodCallIgnored")
    public void prepareGraphDirectoryForStart() throws UnsuitableDirectoryException
    {
        File databaseDirectory = new File( serverConfigurator.getDatabaseDirectory() );
        verifyGraphDirectory( databaseDirectory );
        if ( !databaseDirectory.exists() )
        {
            databaseDirectory.mkdirs();
        }

        File configurationFile = serverConfigurator.getDatabaseConfigurationFile();
        if ( !configurationFile.exists() )
        {
            try
            {
                writeDefaultDatabaseConfiguration( configurationFile );
            }
            catch ( IOException e )
            {
                throw new UnsuitableDirectoryException( "Unable to write default configuration to %s",
                        databaseDirectory );
            }
        }
    }

    private static void verifyGraphDirectory( File dir ) throws UnsuitableDirectoryException
    {
        if ( !dir.isDirectory() )
        {
            throw new UnsuitableDirectoryException( "%s is not a directory", dir );
        }

        if ( !dir.canWrite() )
        {
            throw new UnsuitableDirectoryException( "%s is not writeable", dir );
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

        throw new UnsuitableDirectoryException(
                "%s is neither empty nor does it contain a neo4j graph database", dir );
    }

    public void register( DesktopModelListener desktopModelListener )
    {
        listeners.add( desktopModelListener );
    }

    public void editFile( File file ) throws IOException
    {
        installation.getEnvironment().editFile( file );
    }

    public void openBrowser( String url ) throws IOException, URISyntaxException
    {
        installation.getEnvironment().openBrowser( url );
    }

    public void writeDefaultDatabaseConfiguration( File file ) throws IOException
    {
        InputStream defaults = installation.getDefaultDatabaseConfiguration();
        writeInto( file, defaults );
    }

    public void writeDefaultServerConfiguration( File file ) throws IOException
    {
        InputStream defaults = installation.getDefaultServerConfiguration();
        writeInto( file, defaults );
    }

    private void writeInto( File file, InputStream data ) throws IOException
    {
        if ( data == null )
        {
            // Don't bother writing any files if we somehow don't have any default data for them
            return;
        }

        try ( BufferedReader reader = new BufferedReader( new InputStreamReader( data ) );
              PrintWriter writer = new PrintWriter( file ) )
        {
            String input = reader.readLine();
            while ( input != null )
            {
                writer.println( input );
                input = reader.readLine();
            }
        }
    }

    public File getPluginsDirectory()
    {
        return installation.getPluginsDirectory();
    }

    public void openDirectory( File directory ) throws IOException
    {
        installation.getEnvironment().openDirectory( directory );
    }

    public void launchCommandPrompt() throws IOException, URISyntaxException
    {
        installation.getEnvironment().openCommandPrompt(
                installation.getInstallationBinDirectory(),
                installation.getInstallationJreBinDirectory(),
                installation.getDatabaseDirectory().getParentFile() );
    }
}
