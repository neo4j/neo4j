/**
 * Copyright (c) 2002-2013 "Neo Technology,"
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
package org.neo4j.kernel.impl.nioneo.store;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Reader;
import java.io.Writer;
import java.nio.channels.FileChannel;

public interface FileSystemAbstraction
{
    FileChannel open( File fileName, String mode ) throws IOException;
    
    OutputStream openAsOutputStream( File fileName, boolean append ) throws IOException;
    
    InputStream openAsInputStream( File fileName ) throws IOException;
    
    Reader openAsReader( File fileName, String encoding ) throws IOException;
    
    Writer openAsWriter( File fileName, String encoding, boolean append ) throws IOException;
    
    FileLock tryLock( File fileName, FileChannel channel ) throws IOException;
    
    FileChannel create( File fileName ) throws IOException;
    
    boolean fileExists( File fileName );
    
    boolean mkdir( File fileName );
    
    boolean mkdirs( File fileName );
    
    long getFileSize( File fileName );

    boolean deleteFile( File fileName );
    
    void deleteRecursively( File directory ) throws IOException;
    
    boolean renameFile( File from, File to ) throws IOException;

    // TODO change the name to something more descriptive
    void autoCreatePath( File path ) throws IOException;
    
    File[] listFiles( File directory );
    
    boolean isDirectory( File file );
    
    void moveToDirectory( File file, File toDirectory ) throws IOException;
    
    void copyFile( File from, File to ) throws IOException;
    
    void copyRecursively( File fromDirectory, File toDirectory ) throws IOException;
}
