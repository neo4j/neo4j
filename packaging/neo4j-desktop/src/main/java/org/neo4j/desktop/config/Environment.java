/**
 * Copyright (c) 2002-2013 "Neo Technology,"
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
package org.neo4j.desktop.config;

import java.awt.Desktop;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Properties;

import static java.awt.Desktop.getDesktop;
import static java.awt.Desktop.isDesktopSupported;
import static java.lang.Runtime.getRuntime;
import static java.lang.String.format;

import static org.neo4j.desktop.ui.Components.alert;

public class Environment
{
    private final File appFile;
    Properties desktopProperties;

    public Environment() throws URISyntaxException
    {
        appFile = new File( Environment.class.getProtectionDomain().getCodeSource().getLocation().toURI() );
        desktopProperties = loadDesktopProperties();
    }

    public File getBaseDirectory()
    {
        return appFile != null ? appFile.getParentFile() : new File( "." ).getAbsoluteFile().getParentFile();
    }

    public void openBrowser( String link )
    {
        if ( desktopSupports( Desktop.Action.BROWSE ) )
        {
            try
            {
                getDesktop().browse( new URI( link ) );
            }
            catch ( IOException e )
            {
                e.printStackTrace( System.out );
            }
            catch ( URISyntaxException e )
            {
                e.printStackTrace( System.out );
            }
        }
        else
        {
            throw new UnsupportedOperationException();
        }
    }

    public void editFile( File file ) throws IOException
    {
        if ( desktopSupports( Desktop.Action.EDIT ) )
        {
            try
            {
                if ( !file.exists() )
                {
                    file.createNewFile();
                }

                getDesktop().edit( file );

                return;
            }
            catch ( IOException e )
            {
                // fall through to alert
                e.printStackTrace( System.out );
            }
        }
        else if ( OperatingSystemFamily.WINDOWS.isDetected() )
        {
            getRuntime().exec( new String[]{"rundll32", "url.dll,FileProtocolHandler", file.getAbsolutePath()} );
            return;
        }

        alert( format( "Could not edit file %s", file.getAbsoluteFile() ) ) ;
    }

    public Properties desktopProperties() 
    {
        return desktopProperties;
    }

    private boolean desktopSupports( Desktop.Action action )
    {
        return isDesktopSupported() && getDesktop().isSupported( action );
    }


    private Properties loadDesktopProperties() {
        Properties properties = new Properties() ;
        try {
            InputStream is =  ClassLoader.getSystemResourceAsStream("desktop.properties");
            properties.load(is);
        } catch (IOException ioe) {
            properties.setProperty("desktop.url.path", "/");
        }
        return properties;
    }
}
