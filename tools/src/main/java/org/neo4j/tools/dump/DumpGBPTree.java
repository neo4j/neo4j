/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
