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
package org.neo4j.test;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

public class Unzip
{
    public static File unzip( Class<?> testClass, String resource, File targetDirectory ) throws IOException
    {
        InputStream source = testClass.getResourceAsStream( resource );
        if ( source == null )
        {
            throw new FileNotFoundException( "Could not find resource '" + resource + "' to unzip" );
        }

        try
        {
            ZipInputStream zipStream = new ZipInputStream( source );
            ZipEntry entry = null;
            byte[] scratch = new byte[8096];
            while ( (entry = zipStream.getNextEntry()) != null )
            {
                if ( entry.isDirectory() )
                {
                    new File( targetDirectory, entry.getName() ).mkdirs();
                }
                else
                {
                    OutputStream file = new BufferedOutputStream(
                            new FileOutputStream( new File( targetDirectory, entry.getName() ) ) );
                    try
                    {
                        long toCopy = entry.getSize();
                        while ( toCopy > 0 )
                        {
                            int read = zipStream.read( scratch );
                            file.write( scratch, 0, read );
                            toCopy -= read;
                        }
                    }
                    finally
                    {
                        file.close();
                    }
                }
                zipStream.closeEntry();
            }
        }
        finally
        {
            source.close();
        }
        return targetDirectory;
    }
}
