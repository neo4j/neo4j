/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;

import org.neo4j.desktop.config.Installation;
import org.neo4j.desktop.model.exceptions.UnsuitableDirectoryException;
import org.neo4j.desktop.runtime.DesktopConfigurator;
import org.neo4j.desktop.ui.DesktopModelListener;
import org.neo4j.helpers.HostnamePort;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.internal.Version;

import static java.lang.String.format;

public class DesktopModel
{
    private final Installation installation;
    private final DesktopConfigurator serverConfigurator;
    private final List<DesktopModelListener> listeners = new ArrayList<>();

    public DesktopModel( Installation installation )
    {
        this.installation = installation;
        this.serverConfigurator = new DesktopConfigurator( installation, installation.getDatabaseDirectory() );
    }

    public Config getConfig()
    {
        serverConfigurator.refresh();

        for(DesktopModelListener listener : listeners)
        {
            listener.desktopModelChanged(this);
        }

        return serverConfigurator.configuration();
    }

    public String getNeo4jVersion()
    {
        return format( "%s", Version.getKernel().getReleaseVersion() );
    }

    public HostnamePort getServerAddress()
    {
        return serverConfigurator.getServerAddress();
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
        return installation.getConfigurationsFile();
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

        File configurationFile = installation.getConfigurationsFile();
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
            throw new UnsuitableDirectoryException( "%s is not writable", dir );
        }

        String[] fileNames = dir.list( ( dir1, name ) -> ! name.startsWith( "." ) );

        if ( 0 == fileNames.length )
        {
            return;
        }

        for ( String fileName : fileNames )
        {
            if ( fileName.startsWith( "neostore" ) || fileName.equals( "neo4j.conf" ) )
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
        installation.getEnvironment().browse( url );
    }

    public void writeDefaultDatabaseConfiguration( File file ) throws IOException
    {
        InputStream defaults = installation.getDefaultDatabaseConfiguration();
        writeInto( file, defaults );
    }

    private void writeInto( File file, InputStream data ) throws IOException
    {
        if ( data != null )
        {
            try( BufferedReader reader = new BufferedReader( new InputStreamReader( data ) );
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
