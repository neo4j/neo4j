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
package org.neo4j.desktop.config.portable;

import java.awt.Desktop;
import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import org.neo4j.desktop.config.Environment;

import static java.awt.Desktop.getDesktop;
import static java.awt.Desktop.isDesktopSupported;

public abstract class PortableEnvironment implements Environment
{
    protected void portableEditFile( File file ) throws IOException
    {
        if ( !file.exists() )
        {
            file.createNewFile();
        }

        getDesktop().edit( file );
    }

    protected boolean isPortableEditFileSupported()
    {
        return desktopSupports( Desktop.Action.EDIT );
    }

    protected boolean desktopSupports( Desktop.Action action )
    {
        return isDesktopSupported() && getDesktop().isSupported( action );
    }

    protected boolean isPortableBrowseSupported()
    {
        return desktopSupports( Desktop.Action.BROWSE );
    }

    protected void portableBrowse( String link ) throws IOException, URISyntaxException
    {
        getDesktop().browse( new URI( link ) );
    }

    protected boolean isPortableOpenSupported()
    {
        return desktopSupports( Desktop.Action.OPEN );
    }

    protected void portableOpen( File file ) throws IOException
    {
        getDesktop().open( file );
    }
}
