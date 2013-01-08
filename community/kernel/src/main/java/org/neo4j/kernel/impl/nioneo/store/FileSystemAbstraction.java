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
import java.nio.channels.FileChannel;

public interface FileSystemAbstraction
{
    FileChannel open( File fileName, String mode ) throws IOException;
    
    FileLock tryLock( File fileName, FileChannel channel ) throws IOException;
    
    FileChannel create( File fileName ) throws IOException;
    
    boolean fileExists( File fileName );
    
    long getFileSize( File fileName );

    boolean deleteFile( File fileName );
    
    boolean renameFile( File from, File to ) throws IOException;

    void copyFile( File from, File to ) throws IOException;

    // TODO change the name to something more descriptive
    void autoCreatePath( File path ) throws IOException;
    
//    String[] listFiles( String directory );
}
