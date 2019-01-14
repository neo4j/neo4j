/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * GNU AFFERO GENERAL PUBLIC LICENSE Version 3
 * (http://www.fsf.org/licensing/licenses/agpl-3.0.html) with the
 * Commons Clause, as found in the associated LICENSE.txt file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * Neo4j object code can be licensed independently from the source
 * under separate terms from the AGPL. Inquiries can be directed to:
 * licensing@neo4j.com
 *
 * More information is also available at:
 * https://neo4j.com/licensing/
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
