/*
 * Copyright (c) 2002-2020 "Neo4j,"
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

import java.io.IOException;
import java.nio.file.CopyOption;
import java.nio.file.Path;

class WrappingFileHandle implements FileHandle
{
    private final Path file;
    private final Path baseDirectory;
    private final FileSystemAbstraction fs;

    WrappingFileHandle( Path file, Path baseDirectory, FileSystemAbstraction fs )
    {
        this.file = file;
        this.baseDirectory = baseDirectory;
        this.fs = fs;
    }

    @Override
    public Path getPath()
    {
        return file;
    }

    @Override
    public Path getRelativePath()
    {
        return baseDirectory.relativize( file );
    }

    @Override
    public void rename( Path to, CopyOption... options ) throws IOException
    {
        Path parentFile = file.getParent();
        Path canonicalTarget = to.normalize();
        fs.mkdirs( canonicalTarget.getParent() );
        fs.renameFile( file, canonicalTarget, options );
        removeEmptyParent( parentFile );
    }

    private void removeEmptyParent( Path parentFile )
    {
        // delete up to and including the base directory, but not above.
        // Note that this may be 'null' if 'baseDirectory' is the top directory.
        // Fortunately, 'File.equals(other)' handles 'null' and returns 'false' when 'other' is 'null'.
        Path end = baseDirectory.getParent();
        while ( parentFile != null && !parentFile.equals( end ) )
        {
            Path[] files = fs.listFiles( parentFile );
            if ( files == null || files.length > 0 )
            {
                return;
            }
            fs.deleteFile( parentFile );
            parentFile = parentFile.getParent();
        }
    }

    @Override
    public void delete() throws IOException
    {
        Path parent = file.getParent();
        fs.deleteFileOrThrow( file );
        removeEmptyParent( parent );
    }
}
