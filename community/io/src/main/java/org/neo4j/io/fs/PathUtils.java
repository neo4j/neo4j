/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
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
package org.neo4j.io.fs;

import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;

import static java.nio.file.FileVisitResult.CONTINUE;
import static java.nio.file.Files.delete;
import static java.nio.file.Files.isDirectory;
import static java.nio.file.Files.walkFileTree;
import static java.util.Objects.requireNonNull;
import static org.neo4j.util.Preconditions.checkState;

public final class PathUtils
{
    private static final SimpleFileVisitor<Path> WALKING_DELETE = new SimpleFileVisitor<>()
    {
        @Override
        public FileVisitResult visitFile( Path file, BasicFileAttributes attrs ) throws IOException
        {
            delete( file );
            return CONTINUE;
        }

        @Override
        public FileVisitResult postVisitDirectory( Path dir, IOException exc ) throws IOException
        {
            if ( exc != null )
            {
                throw exc;
            }
            delete( dir );
            return CONTINUE;
        }
    };

    private PathUtils()
    {
    }

    public static boolean isEmpty( Path directory ) throws IOException
    {
        try ( DirectoryStream<Path> dirStream = Files.newDirectoryStream( directory ) )
        {
            return !dirStream.iterator().hasNext();
        }
    }

    public static void deleteDirectory( Path directory ) throws IOException
    {
        requireNonNull( directory );
        checkState( isDirectory( directory ), "No directory supplied" );

        walkFileTree( directory, WALKING_DELETE );
    }
}
