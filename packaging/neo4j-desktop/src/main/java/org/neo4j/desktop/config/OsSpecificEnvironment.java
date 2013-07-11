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
import java.awt.Desktop.Action;
import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

/**
 * Provides OS-specific implementations of {@link Environment} interface.
 */
public class OsSpecificEnvironment extends OsSpecific<Environment>
{
    @Override
    protected Environment getFor( Os os )
    {
        switch ( os )
        {
        case WINDOWS:
            return new WindowsEnvironment();
        default:
            throw new UnsupportedOperationException( os.name() );
        }
    }
    
    static class WindowsEnvironment implements Environment
    {
        private final File appFile;
        
        WindowsEnvironment()
        {
            String firstEntry = System.getProperty( "java.class.path" ).split( ";" )[0];
            appFile = firstEntry.toLowerCase().endsWith( ".exe" ) ? new File( firstEntry ).getAbsoluteFile() : null;
        }
        
        @Override
        public boolean isRunByApp()
        {
            return appFile != null;
        }
        
        @Override
        public File getAppFile()
        {
            return appFile;
        }

        @Override
        public File getExtensionsDirectory()
        {
            return new File( getBaseDirectory(), "extensions" );
        }
        
        private File getBaseDirectory()
        {
            return appFile != null ?
                appFile.getParentFile() :
                new File( "." ).getAbsoluteFile().getParentFile();
        }
        
        @Override
        public void openBrowser( String link )
        {
            Desktop desktop = getDesktop( Action.BROWSE );
            if ( desktop != null )
            {
                try
                {
                    desktop.browse( new URI( link ) );
                }
                catch ( IOException e )
                {
                    e.printStackTrace();
                }
                catch ( URISyntaxException e )
                {
                    e.printStackTrace();
                }
            }
            else
            {
                throw new UnsupportedOperationException();
            }
        }

        @Override
        public void editFile( File file ) throws IOException
        {
            Desktop desktop = getDesktop( Action.EDIT );
            if ( desktop != null )
            {
                try
                {
                    desktop.edit( file );
                    return;
                }
                catch ( IOException e )
                {   // Fallback below
                }
            }
            
            Runtime.getRuntime().exec( new String[] {
                    "cmd",
                    "\"/C start " + file.getAbsolutePath() + "\""
            } );
        }

        private Desktop getDesktop( Action action )
        {
            if ( Desktop.isDesktopSupported() )
            {
                Desktop desktop = Desktop.getDesktop();
                if ( desktop.isSupported( Action.BROWSE ) )
                {
                    return desktop;
                }
            }
            return null;
        }
    }
}
