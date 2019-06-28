/*
 * Copyright (c) 2002-2019 "Neo4j,"
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

import org.junit.jupiter.api.Test;
import org.mockito.Mock;

import java.io.IOException;
import java.nio.file.FileVisitor;
import java.nio.file.Path;
import java.nio.file.Paths;

import org.neo4j.function.ThrowingConsumer;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.neo4j.io.fs.FileVisitors.onFile;

@SuppressWarnings( "unchecked" )
class OnFileTest
{
    private final ThrowingConsumer<Path, IOException> operation = mock( ThrowingConsumer.class );
    private final FileVisitor<Path> wrapped = mock( FileVisitor.class );

    @Test
    void shouldNotOperateOnDirectories() throws IOException
    {
        Path dir = Paths.get( "/some/path" );
        onFile( operation, wrapped ).preVisitDirectory( dir, null );
        verify( operation, never() ).accept( dir );
    }

    @Test
    void shouldOperateOnFiles() throws IOException
    {
        Path file = Paths.get( "/some/path" );
        onFile( operation, wrapped ).visitFile( file, null );
        verify( operation ).accept( file );
    }
}
