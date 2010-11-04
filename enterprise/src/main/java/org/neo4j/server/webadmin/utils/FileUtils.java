/**
 * Copyright (c) 2002-2010 "Neo Technology,"
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

package org.neo4j.server.webadmin.utils;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;

/**
 * Utilities for manipulating files.
 * 
 * @author Jacob Hansson <jacob@voltvoodoo.com>
 * 
 */
public class FileUtils
{

    /**
     * Recursively destroy an entire directory.
     * 
     * @param directory
     */
    public static void delTree( File file )
    {
        for ( File childFile : file.listFiles() )
        {
            if ( childFile.isDirectory() )
            {
                delTree( childFile );
            }
            childFile.delete();
        }
    }

    public static final byte[] getFileAsBytes( final File file )
            throws IOException
    {
        final BufferedInputStream bis = new BufferedInputStream(
                new FileInputStream( file ) );
        final byte[] bytes = new byte[(int) file.length()];
        bis.read( bytes );
        bis.close();
        return bytes;
    }

    public static final String getFileAsString( final File file )
            throws IOException
    {
        return new String( getFileAsBytes( file ) );
    }

}
