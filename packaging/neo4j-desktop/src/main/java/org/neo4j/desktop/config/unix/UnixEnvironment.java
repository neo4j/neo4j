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
package org.neo4j.desktop.config.unix;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;

import org.neo4j.desktop.config.portable.PortableEnvironment;

class UnixEnvironment extends PortableEnvironment
{
    @Override
    public void openBrowser( String url ) throws IOException, URISyntaxException
    {
        if ( isPortableBrowseSupported() )
        {
            portableBrowse( url );
            return;
        }

        throw new UnsupportedOperationException( "Cannot browse to URL: " + url );
    }

    @Override
    public void editFile( File file ) throws IOException
    {
        if ( isPortableEditFileSupported() )
        {
            portableEditFile( file );
            return;
        }

        throw new UnsupportedOperationException( "Cannot edit file: " + file );
    }

    @Override
    public void openDirectory( File directory ) throws IOException
    {
        if ( isPortableOpenSupported() )
        {
            portableOpen( directory );
            return;
        }

        throw new UnsupportedOperationException( "Cannot open directory: " + directory );
    }

    @Override
    public void openCommandPrompt( File binDirectory, File jreBinDirectory, File workingDirectory ) throws IOException
    {
        throw new UnsupportedOperationException(
                "Opening a command prompt is not currently supported on this operating system." );
    }
}
