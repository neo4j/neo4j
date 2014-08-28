/**
 * Copyright (c) 2002-2014 "Neo Technology,"
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
package org.neo4j.index.impl.lucene;

import java.io.File;
import java.io.IOException;
import java.util.TimeZone;

import org.neo4j.helpers.Args;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.DefaultFileSystemAbstraction;

public class DumpLogicalLog extends org.neo4j.kernel.impl.util.DumpLogicalLog
{
    public DumpLogicalLog( FileSystemAbstraction fileSystem )
    {
        super( fileSystem );
    }

    public static void main( String[] args ) throws IOException
    {
        FileSystemAbstraction fs = new DefaultFileSystemAbstraction();
        Args arguments = new Args( args );
        TimeZone timeZome = parseTimeZoneConfig( arguments );
        try ( Printer printer = getPrinter( arguments ) )
        {
            for ( String file : arguments.orphans() )
            {
                int dumped = new DumpLogicalLog( fs ).dump( file, "lucene.log", printer.getFor( file ), timeZome );
                if ( dumped == 0 && isAGraphDatabaseDirectory( file ) )
                {   // If none were found and we really pointed to a neodb directory
                    // then go to its index folder and try there.
                    String path = new File( file, "index" ).getAbsolutePath();
                    new DumpLogicalLog( fs ).dump( path, "lucene.log", printer.getFor( file ), timeZome );
                }
            }
        }
    }
}
