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
package org.neo4j.com.storecopy;

import java.io.File;
import java.io.IOException;
import java.nio.file.CopyOption;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Optional;

import org.neo4j.io.fs.FileHandle;
import org.neo4j.io.pagecache.PageCache;

@FunctionalInterface
public interface FileMoveAction
{
    void move( File toDir, CopyOption... copyOptions ) throws IOException;

    static FileMoveAction copyViaPageCache( File file, PageCache pageCache )
    {
        return ( toDir, copyOptions ) ->
        {
            Optional<FileHandle> handle = pageCache.getCachedFileSystem().streamFilesRecursive( file ).findAny();
            if ( handle.isPresent() )
            {
                handle.get().rename( new File( toDir, file.getName() ), copyOptions );
            }
        };
    }

    static FileMoveAction copyViaFileSystem( File file, File basePath )
    {
        Path base = basePath.toPath();
        return ( toDir, copyOptions ) ->
        {
            Path originalPath = file.toPath();
            Path relativePath = base.relativize( originalPath );
            Path resolvedPath = toDir.toPath().resolve( relativePath );
            Files.createDirectories( resolvedPath.getParent() );
            Files.copy( originalPath, resolvedPath, copyOptions );
        };
    }

}
