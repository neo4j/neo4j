/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
package org.neo4j.kernel.impl.index.schema;

import java.io.File;
import java.io.IOException;
import java.nio.file.NoSuchFileException;

import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.impl.index.GBPTreeFileUtil;

public class GBPTreeFileSystemFileUtil implements GBPTreeFileUtil
{
    private final FileSystemAbstraction fs;

    public GBPTreeFileSystemFileUtil( FileSystemAbstraction fs )
    {
        this.fs = fs;
    }

    @Override
    public void deleteFile( File storeFile ) throws IOException
    {
        fs.deleteFileOrThrow( storeFile );
    }

    @Override
    public void deleteFileIfPresent( File storeFile ) throws IOException
    {
        try
        {
            deleteFile( storeFile );
        }
        catch ( NoSuchFileException e )
        {
            // File does not exist, we don't need to delete
        }
    }

    @Override
    public boolean storeFileExists( File storeFile )
    {
        return fs.fileExists( storeFile );
    }

    @Override
    public void mkdirs( File dir ) throws IOException
    {
        fs.mkdirs( dir );
    }
}
