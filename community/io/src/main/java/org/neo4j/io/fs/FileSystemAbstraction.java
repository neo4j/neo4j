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
package org.neo4j.io.fs;

import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Reader;
import java.io.Writer;
import java.util.zip.ZipOutputStream;

import org.neo4j.function.Function;

public interface FileSystemAbstraction
{
    StoreChannel open( File fileName, String mode ) throws IOException;

    OutputStream openAsOutputStream( File fileName, boolean append ) throws IOException;

    InputStream openAsInputStream( File fileName ) throws IOException;

    Reader openAsReader( File fileName, String encoding ) throws IOException;

    Writer openAsWriter( File fileName, String encoding, boolean append ) throws IOException;

    StoreChannel create( File fileName ) throws IOException;

    boolean fileExists( File fileName );

    boolean mkdir( File fileName );

    void mkdirs( File fileName ) throws IOException;

    long getFileSize( File fileName );

    boolean deleteFile( File fileName );

    void deleteRecursively( File directory ) throws IOException;

    boolean renameFile( File from, File to ) throws IOException;

    File[] listFiles( File directory );

    File[] listFiles( File directory, FilenameFilter filter );

    boolean isDirectory( File file );

    void moveToDirectory( File file, File toDirectory ) throws IOException;

    void copyFile( File from, File to ) throws IOException;

    void copyRecursively( File fromDirectory, File toDirectory ) throws IOException;

    <K extends ThirdPartyFileSystem> K getOrCreateThirdPartyFileSystem( Class<K> clazz, Function<Class<K>, K> creator );

    void truncate( File path, long size ) throws IOException;

    interface ThirdPartyFileSystem
    {
        void close();

        void dumpToZip( ZipOutputStream zip, byte[] scratchPad ) throws IOException;
    }
}
