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

import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.util.List;

import org.neo4j.helpers.collection.ClosableIterator;

import static org.neo4j.helpers.collection.IteratorUtil.asIterator;

public class FileSystemListIO implements ListIO
{
    private static final String ENCODING = "UTF-8";

    @Override
    public List<String> read( List<String> target, File file ) throws IOException
    {
        if ( file.exists() )
        {
            ClosableIterator<String> iterator = asIterator( file, ENCODING );
            try
            {
                while ( iterator.hasNext() )
                {
                    target.add( iterator.next() );
                }
            }
            finally
            {
                iterator.close();
            }
        }
        return target;
    }

    @Override
    public void write( List<String> list, File file ) throws IOException
    {
        PrintStream writer = null;
        try
        {
            writer = new PrintStream( file, ENCODING );
            for ( String line : list )
            {
                writer.println( line );
            }
        }
        finally
        {
            if ( writer != null )
            {
                writer.close();
            }
        }
    }
}
