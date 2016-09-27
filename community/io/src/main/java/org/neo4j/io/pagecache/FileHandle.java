/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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
package org.neo4j.io.pagecache;

import java.io.File;
import java.io.IOException;
import java.nio.file.CopyOption;

public interface FileHandle
{
    String getAbsolutePath();

    File getFile();

    /**
     * Rename source file to the given target file, effectively moving the file from source to target.
     *
     * Both files have to be unmapped when performing the rename, otherwise an exception will be thrown.
     *
     * @param to The new name of the file after the rename.
     * @param options Options to modify the behaviour of the move in possibly platform specific ways. In particular,
     * {@link java.nio.file.StandardCopyOption#REPLACE_EXISTING} may be used to overwrite any existing file at the
     * target path name, instead of throwing an exception.
     */
    void renameFile( File to, CopyOption... options ) throws IOException;

    void delete() throws IOException;
}
