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
package org.apache.lucene.index;

import java.io.File;

import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;

public class RebuildCompoundFile
{
    public static void main( String[] args ) throws Exception
    {
        if(args.length != 1)
        {
            System.out.println("Usage: RebuildCompoundFile <INDEX DIRECTORY>");
            System.exit( 0 );
        }
        String path = args[0];
        File baseDirectory = new File( path );
        Directory directory = FSDirectory.open( baseDirectory );
        CompoundFileWriter writer = new CompoundFileWriter( directory, "_drr.cfs" );
        for ( String pathname : directory.listAll() )
        {
            if ( pathname.endsWith( "gen" ) || pathname.endsWith( "cfs" ) || pathname.endsWith( "12" ) )
            {
                continue;
            }
            writer.addFile( pathname );
        }
        writer.close();
    }
}
