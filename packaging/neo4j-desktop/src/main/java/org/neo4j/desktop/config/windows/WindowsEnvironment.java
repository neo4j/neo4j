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
package org.neo4j.desktop.config.windows;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;

import org.neo4j.desktop.config.portable.PortableEnvironment;

import static java.lang.Runtime.getRuntime;

class WindowsEnvironment extends PortableEnvironment
{
    @Override
    public void openBrowser( String link ) throws IOException, URISyntaxException
    {
        if ( isPortableBrowseSupported() )
        {
            portableBrowse( link );
            return;
        }

        throw new UnsupportedOperationException();
    }

    @Override
    public void editFile( File file ) throws IOException
    {
        if ( isPortableEditFileSupported() )
        {
            try
            {
                portableEditFile( file );
                return;
            }
            catch ( IOException e )
            {
                e.printStackTrace( System.out );
            }
        }
        windowsEditFile( file );
    }

    private void windowsEditFile( File file ) throws IOException
    {
        String[] cmdarray = { "notepad", file.getAbsolutePath() };
        getRuntime().exec( cmdarray );
    }

    @Override
    public void openDirectory( File directory ) throws IOException
    {
        if ( isPortableOpenSupported() )
        {
            try
            {
                portableOpen( directory );
                return;
            }
            catch ( IOException e )
            {
                e.printStackTrace( System.out );
            }
        }

        windowsOpenDirectory( directory );
    }

    private void windowsOpenDirectory( File directory ) throws IOException
    {
        getRuntime().exec( new String[] { "explorer", directory.getAbsolutePath() } );
    }
}
