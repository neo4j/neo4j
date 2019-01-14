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
package org.neo4j.io.file;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;

import org.neo4j.io.fs.FileSystemAbstraction;

/**
 * This class consists exclusively of static methods that operate on files, directories, or other types of files.
 */
public class Files
{
    private Files()
    {
    }

    /**
     * Creates a file, or opens an existing file. If necessary, parent directories will be created.
     *
     * @param fileSystem The filesystem abstraction to use
     * @param file The file to create or open
     * @return An output stream
     * @throws IOException If an error occurs creating directories or opening the file
     */
    public static OutputStream createOrOpenAsOutputStream( FileSystemAbstraction fileSystem, File file, boolean append ) throws IOException
    {
        if ( file.getParentFile() != null )
        {
            fileSystem.mkdirs( file.getParentFile() );
        }
        return fileSystem.openAsOutputStream( file, append );
    }
}
