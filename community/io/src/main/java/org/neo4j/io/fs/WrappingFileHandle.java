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

import java.io.File;
import java.io.IOException;
import java.nio.file.CopyOption;

class WrappingFileHandle implements FileHandle
{
    private final File file;
    private final File baseDirectory;
    private final FileSystemAbstraction fs;

    WrappingFileHandle( File file, File baseDirectory, FileSystemAbstraction fs )
    {
        this.file = file;
        this.baseDirectory = baseDirectory;
        this.fs = fs;
    }

    @Override
    public File getFile()
    {
        return file;
    }

    @Override
    public File getRelativeFile()
    {
        int baseLength = baseDirectory.getPath().length();
        if ( baseDirectory.getParent() != null )
        {
            baseLength++;
        }
        return new File( file.getPath().substring( baseLength ) );
    }

    @Override
    public void rename( File to, CopyOption... options ) throws IOException
    {
        File parentFile = file.getParentFile();
        File cannonicalTarget = to.getCanonicalFile();
        fs.mkdirs( cannonicalTarget.getParentFile() );
        fs.renameFile( file, cannonicalTarget, options );
        removeEmptyParent( parentFile );
    }

    private void removeEmptyParent( File parentFile )
    {
        // delete up to and including the base directory, but not above.
        // Note that this may be 'null' if 'baseDirectory' is the top directory.
        // Fortunately, 'File.equals(other)' handles 'null' and returns 'false' when 'other' is 'null'.
        File end = baseDirectory.getParentFile();
        while ( parentFile != null && !parentFile.equals( end ) )
        {
            File[] files = fs.listFiles( parentFile );
            if ( files == null || files.length > 0 )
            {
                return;
            }
            fs.deleteFile( parentFile );
            parentFile = parentFile.getParentFile();
        }
    }

    @Override
    public void delete() throws IOException
    {
        File parentFile = file.getParentFile();
        fs.deleteFileOrThrow( file );
        removeEmptyParent( parentFile );
    }
}
