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
import org.apache.lucene.store.IndexInput;

public class RebuildSegmentInfo
{
    public static void main( String[] args ) throws Exception
    {
        if(args.length != 1)
        {
            System.out.println("Usage: RebuildSegmentInfo <INDEX DIRECTORY>");
            System.exit( 0 );
        }
        String path = args[0];
        File file = new File( path );
        Directory directory = FSDirectory.open( file );

        SegmentInfos infos = new SegmentInfos();
        int counter = 0;
        for ( String fileName : directory.listAll() )
        {
            if ( directory.fileLength( fileName ) == 0 || !fileName.endsWith( "cfs" ) )
            {
                System.out.println( "Skipping " + fileName + ", size " + directory.fileLength( fileName ) );
                continue;
            }
            else
            {
                System.out.println( "Doing " + fileName + ", size " + directory.fileLength( fileName ) );
            }
            String segmentName = fileName.substring( 1, fileName.lastIndexOf( '.' ) );

            int segmentInt = Integer.parseInt( segmentName, Character.MAX_RADIX );
            counter = Math.max( counter, segmentInt );

            segmentName = fileName.substring( 0, fileName.lastIndexOf( '.' ) );

            Directory fileReader = new CompoundFileReader( directory, fileName );
            IndexInput indexStream = fileReader.openInput( segmentName + ".cfs" );

            SegmentInfo segmentInfo = new SegmentInfo( directory, SegmentInfos.CURRENT_FORMAT, indexStream );
            System.out.println( "Name was: \"" + segmentInfo.name + "\"" );
            System.out.println( "Doc count was: " + segmentInfo.docCount );
            infos.add( segmentInfo );

            // indexStream.close();
            // fileReader.close();
        }

        infos.counter = ++counter;

        infos.commit( directory );
    }
}
