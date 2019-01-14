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
package org.neo4j.io.compress;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.nio.file.FileSystem;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;

import org.neo4j.helpers.collection.MapUtil;
import org.neo4j.io.fs.FileHandle;
import org.neo4j.io.fs.FileSystemAbstraction;

import static java.util.stream.Collectors.toList;

public class ZipUtils
{
    /**
     * Create zip archive for requested <code>sourceToCompress</code>.
     * If <code>sourceToCompress</code> is a directory then content of that directory and all its sub-directories will be added to the archive.
     * If <code>sourceToCompress</code> does not exist or is an empty directory then archive will not be created.
     * @param fileSystem source file system
     * @param sourceToCompress source file to compress
     * @param destinationZip zip file compress source to
     * @throws IOException when underlying file system access produce IOException
     */
    public static void zip( FileSystemAbstraction fileSystem, File sourceToCompress, File destinationZip ) throws IOException
    {
        if ( !fileSystem.fileExists( sourceToCompress ) )
        {
            return;
        }
        if ( isEmptyDirectory( fileSystem, sourceToCompress ) )
        {
            return;
        }
        Map<String,String> env = MapUtil.stringMap( "create", "true" );
        Path rootPath = sourceToCompress.toPath();
        URI archiveAbsoluteURI = URI.create( "jar:file:" + destinationZip.toURI().getRawPath() );

        try ( FileSystem zipFs = FileSystems.newFileSystem( archiveAbsoluteURI, env ) )
        {
            List<FileHandle> fileHandles = fileSystem.streamFilesRecursive( sourceToCompress ).collect( toList() );
            for ( FileHandle fileHandle : fileHandles )
            {
                Path sourcePath = fileHandle.getFile().toPath();
                Path zipFsPath = fileSystem.isDirectory( sourceToCompress ) ? zipFs.getPath( rootPath.relativize( sourcePath ).toString() )
                                                                            : zipFs.getPath( sourcePath.getFileName().toString() );
                if ( zipFsPath.getParent() != null )
                {
                    Files.createDirectories( zipFsPath.getParent() );
                }
                Files.copy( sourcePath, zipFsPath );
            }
        }
    }

    private static boolean isEmptyDirectory( FileSystemAbstraction fileSystem, File sourceToCompress )
    {
        if ( fileSystem.isDirectory( sourceToCompress ) )
        {
            File[] files = fileSystem.listFiles( sourceToCompress );
            return files == null || files.length == 0;
        }
        return false;
    }
}
