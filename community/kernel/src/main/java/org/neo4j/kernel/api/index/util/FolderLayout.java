/*
 * Copyright (c) 2002-2018 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
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
package org.neo4j.kernel.api.index.util;

import java.io.File;
import java.io.FileFilter;

import static java.lang.Integer.parseInt;

public class FolderLayout implements FileFilter
{
    private final File rootDirectory;

    public FolderLayout( File rootDirectory )
    {
        this.rootDirectory = rootDirectory;
    }

    public File getFolder( long indexId )
    {
        return new File( rootDirectory, "" + indexId );
    }

    @Override
    public boolean accept( File path )
    {
        if ( !path.isDirectory() )
        {
            return false;
        }
        try
        {
            parseInt( path.getName() );
            return true;
        }
        catch ( NumberFormatException e )
        {
            return false;
        }
    }
}
