/*
 * Copyright (c) "Neo4j"
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

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.net.URL;
import java.nio.file.FileSystem;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.util.Enumeration;
import java.util.List;
import java.util.Map;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;

import org.neo4j.io.fs.FileHandle;
import org.neo4j.io.fs.FileSystemAbstraction;

import static java.util.stream.Collectors.toList;

public class ZipUtils
{
    /**
     * Like {@link #zip(FileSystemAbstraction, Path, Path, boolean)}, with includeSourceFolder=false.
     */
    public static void zip( FileSystemAbstraction fileSystem, Path sourceToCompress, Path destinationZip ) throws IOException
    {
        zip( fileSystem, sourceToCompress, destinationZip, false );
    }

    /**
     * Create zip archive for requested <code>sourceToCompress</code>.
     * If <code>sourceToCompress</code> is a directory then content of that directory and all its sub-directories will be added to the archive.
     * If <code>sourceToCompress</code> does not exist or is an empty directory then archive will not be created.
     * @param fileSystem source file system
     * @param sourceToCompress source file to compress
     * @param destinationZip zip file compress source to
     * @param includeSourceDirectoryInRelativePath true if relative path of content should include sourceToCompress, otherwise false.
     *                                             This is only meaningful sourceToCompress is a directory.
     * @throws IOException when underlying file system access produce IOException
     */
    public static void zip( FileSystemAbstraction fileSystem, Path sourceToCompress, Path destinationZip, boolean includeSourceDirectoryInRelativePath )
            throws IOException
    {
        if ( !fileSystem.fileExists( sourceToCompress ) )
        {
            return;
        }
        if ( isEmptyDirectory( fileSystem, sourceToCompress ) )
        {
            return;
        }
        Map<String,String> env = Map.of( "create", "true" );
        URI archiveAbsoluteURI = URI.create( "jar:file:" + destinationZip.toUri().getRawPath() );

        Path baseForRelativePath = sourceToCompress;
        if ( includeSourceDirectoryInRelativePath )
        {
            baseForRelativePath = sourceToCompress.getParent();
        }

        try ( FileSystem zipFs = FileSystems.newFileSystem( archiveAbsoluteURI, env ) )
        {
            List<FileHandle> fileHandles = fileSystem.streamFilesRecursive( sourceToCompress ).collect( toList() );
            for ( FileHandle fileHandle : fileHandles )
            {
                Path sourcePath = fileHandle.getPath();
                Path zipFsPath = fileSystem.isDirectory( sourceToCompress ) ? zipFs.getPath( baseForRelativePath.relativize( sourcePath ).toString() )
                                                                            : zipFs.getPath( sourcePath.getFileName().toString() );
                if ( zipFsPath.getParent() != null )
                {
                    Files.createDirectories( zipFsPath.getParent() );
                }
                Files.copy( sourcePath, zipFsPath );
            }
        }
    }

    /**
     * Unzip a zip file located in the resource directly of provided class.
     * The zip file is expected to contain a single file with the same name as target.
     * The content is unpacked into target location.
     *
     * @param klass The class from which to get the zip file resource.
     * @param zipName Name of zip file.
     * @param targetFile Target file to which content will be unzipped, must align with content of zip file.
     * @throws IOException if something goes wrong.
     */
    public static void unzipResource( Class<?> klass, String zipName, Path targetFile ) throws IOException
    {
        URL resource = klass.getResource( zipName );
        if ( resource == null )
        {
            throw new NoSuchFileException( zipName );
        }
        String sourceZip = resource.getFile();
        try ( ZipFile zipFile = new ZipFile( sourceZip ) )
        {
            Enumeration<? extends ZipEntry> entries = zipFile.entries();
            if ( !entries.hasMoreElements() )
            {
                throw new IllegalStateException( "Zip file '" + sourceZip + "' does not contain any elements." );
            }
            ZipEntry entry = entries.nextElement();
            if ( !targetFile.getFileName().toString().equals( entry.getName() ) )
            {
                throw new IllegalStateException( "Zip file '" + sourceZip + "' does not contain target file '" + targetFile.getFileName() + "'." );
            }
            Files.copy( zipFile.getInputStream( entry ), targetFile );
        }
    }

    /**
     * Unzip the source file to targetDirectory.
     *
     * @param sourceZip {@link String} with path pointing at the source zip file.
     * @param targetDirectory {@link Path} defining directory where zip should be extracted.
     * @throws IOException if something goes wrong.
     */
    public static void unzip( String sourceZip, Path targetDirectory ) throws IOException
    {
        try ( ZipFile zipFile = new ZipFile( sourceZip ) )
        {
            Enumeration<? extends ZipEntry> entries = zipFile.entries();
            while ( entries.hasMoreElements() )
            {
                ZipEntry entry = entries.nextElement();
                if ( entry.isDirectory() )
                {
                    Files.createDirectories( targetDirectory.resolve( entry.getName() ) );
                }
                else
                {
                    try ( OutputStream file = new BufferedOutputStream( Files.newOutputStream( targetDirectory.resolve( entry.getName() ) ) );
                          InputStream is = zipFile.getInputStream( entry ) )
                    {
                        int read;
                        is.transferTo( file );
                    }
                }
            }
        }
    }

    private static boolean isEmptyDirectory( FileSystemAbstraction fileSystem, Path sourceToCompress ) throws IOException
    {
        if ( fileSystem.isDirectory( sourceToCompress ) )
        {
            Path[] files = fileSystem.listFiles( sourceToCompress );
            return files.length == 0;
        }
        return false;
    }
}
