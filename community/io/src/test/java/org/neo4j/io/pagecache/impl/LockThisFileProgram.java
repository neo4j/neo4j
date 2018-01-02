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
package org.neo4j.io.pagecache.impl;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;

public class LockThisFileProgram
{
    public static final String LOCKED_OUTPUT = "locked";

    public static void main( String[] args ) throws IOException
    {
        Path path = Paths.get( args[0] );
        try ( FileChannel channel = FileChannel.open( path, StandardOpenOption.READ, StandardOpenOption.WRITE );
              java.nio.channels.FileLock lock = channel.lock() )
        {
            System.out.println( LOCKED_OUTPUT );
            System.out.flush();
            System.in.read();
        }
    }
}
