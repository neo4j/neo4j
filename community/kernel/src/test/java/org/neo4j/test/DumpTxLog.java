/**
 * Copyright (c) 2002-2015 "Neo Technology,"
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

import static org.neo4j.test.LogTestUtils.DUMP;
import static org.neo4j.test.LogTestUtils.filterTxLog;

import java.io.File;
import java.io.IOException;

import org.neo4j.kernel.DefaultFileSystemAbstraction;
import org.neo4j.kernel.impl.nioneo.store.FileSystemAbstraction;

public class DumpTxLog
{
    public static void main( String[] args ) throws IOException
    {
        FileSystemAbstraction fileSystemAbstraction = new DefaultFileSystemAbstraction();
        String file = arg( args, 0, "graph db store directory or file" );
        if ( new File( file ).isDirectory() )
            // Assume store directory
            filterTxLog( fileSystemAbstraction, file, DUMP );
        else
            // Point out a specific file
            filterTxLog( fileSystemAbstraction, new File( file ), DUMP );
    }

    private static String arg( String[] args, int i, String failureMessage )
    {
        if ( i >= args.length )
        {
            System.out.println( "Missing " + failureMessage );
            System.exit( 1 );
        }
        return args[i];
    }
}
