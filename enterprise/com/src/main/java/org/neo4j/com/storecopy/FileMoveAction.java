/*
 * Copyright (c) 2002-2018 "Neo4j,"
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
package org.neo4j.com.storecopy;

import java.io.File;
import java.io.IOException;
import java.nio.file.CopyOption;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Optional;

import org.neo4j.io.pagecache.FileHandle;
import org.neo4j.io.pagecache.PageCache;

@FunctionalInterface
public interface FileMoveAction
{
    void move( File toDir, CopyOption... copyOptions ) throws IOException;

    static FileMoveAction copyViaPageCache( File file, PageCache pageCache )
    {
        return ( toDir, copyOptions ) ->
        {
            Optional<FileHandle> handle = pageCache.streamFilesRecursive( file ).findAny();
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
