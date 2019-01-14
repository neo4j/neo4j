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
package org.neo4j.com.storecopy;

import java.io.File;
import java.io.IOException;
import java.nio.file.CopyOption;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Optional;

import org.neo4j.io.fs.FileHandle;
import org.neo4j.io.pagecache.PageCache;

public interface FileMoveAction
{
    /**
     * Execute a file move, moving the prepared file to the given {@code toDir}.
     * @param toDir The target directory of the move operation
     * @param copyOptions
     * @throws IOException
     */
    void move( File toDir, CopyOption... copyOptions ) throws IOException;

    File file();

    static FileMoveAction moveViaPageCache( File file, PageCache pageCache )
    {
        return new FileMoveAction()
        {
            @Override
            public void move( File toDir, CopyOption... copyOptions ) throws IOException
            {
                Optional<FileHandle> handle = pageCache.getCachedFileSystem().streamFilesRecursive( file ).findAny();
                boolean directoryExistsInCachedSystem = handle.isPresent();
                if ( directoryExistsInCachedSystem )
                {
                    handle.get().rename( new File( toDir, file.getName() ), copyOptions );
                }
            }

            @Override
            public File file()
            {
                return file;
            }
        };
    }

    /**
     * Create a FileMoveAction through the file system for moving the {@code file} which is contained in {@code basePath}.
     *   When executing the move action the {@param file} will be moved to the argument supplied to the {@link #move(File, CopyOption...)} call
     *   as the argument
     * @param file The file to move
     * @param basePath The directory containing the file
     * @return A FileMoveAction for the given file.
     */
    static FileMoveAction copyViaFileSystem( File file, File basePath )
    {
        Path base = basePath.toPath();
        return new FileMoveAction()
        {
            @Override
            public void move( File toDir, CopyOption... copyOptions ) throws IOException
            {
                Path originalPath = file.toPath();
                Path relativePath = base.relativize( originalPath );
                Path resolvedPath = toDir.toPath().resolve( relativePath );
                if ( !Files.isSymbolicLink( resolvedPath.getParent() ) )
                {
                    Files.createDirectories( resolvedPath.getParent() );
                }
                Files.copy( originalPath, resolvedPath, copyOptions );
            }

            @Override
            public File file()
            {
                return file;
            }
        };
    }

    static FileMoveAction moveViaFileSystem( File sourceFile, File sourceDirectory )
    {
        return new FileMoveAction()
        {
            @Override
            public void move( File toDir, CopyOption... copyOptions ) throws IOException
            {
                copyViaFileSystem( sourceFile, sourceDirectory ).move( toDir, copyOptions );
                if ( !sourceFile.delete() )
                {
                    throw new IOException( "Unable to delete source file after copying " + sourceFile );
                }
            }

            @Override
            public File file()
            {
                return sourceFile;
            }
        };
    }
}
