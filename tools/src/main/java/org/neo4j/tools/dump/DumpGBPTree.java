/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * Neo4j Sweden Software License, as found in the associated LICENSE.txt
 * file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * Neo4j Sweden Software License for more details.
 */
package org.neo4j.tools.dump;

import java.io.File;
import java.io.IOException;

import org.neo4j.index.internal.gbptree.GBPTree;
import org.neo4j.index.internal.gbptree.TreePrinter;
import org.neo4j.io.fs.DefaultFileSystemAbstraction;

/**
 * For now only dumps header, could be made more useful over time.
 */
public class DumpGBPTree
{
    /**
     * Dumps stuff about a {@link GBPTree} to console in human readable format.
     *
     * @param args arguments.
     * @throws IOException on I/O error.
     */
    public static void main( String[] args ) throws IOException
    {
        if ( args.length == 0 )
        {
            System.err.println( "File argument expected" );
            System.exit( 1 );
        }

        File file = new File( args[0] );
        System.out.println( "Dumping " + file.getAbsolutePath() );
        TreePrinter.printHeader( new DefaultFileSystemAbstraction(), file, System.out );
    }
}
