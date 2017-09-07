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
package org.neo4j.kernel.impl.index.labelscan;

import java.io.File;
import java.io.IOException;
import java.nio.file.NoSuchFileException;

import org.neo4j.io.fs.FileHandle;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.impl.index.GBPTreeFileUtil;

import static org.neo4j.function.Predicates.alwaysTrue;

/**
 * Use {@link PageCache} to handle file operations on GBPTree file.
 */
public class GBPTreePageCacheFileUtil implements GBPTreeFileUtil
{
    private final PageCache pageCache;

    public GBPTreePageCacheFileUtil( PageCache pageCache )
    {
        this.pageCache = pageCache;
    }

    public void deleteFile( File storeFile ) throws IOException
    {
        FileHandle fileHandle = storeFileHandle( pageCache, storeFile );
        fileHandle.delete();
    }

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

    public boolean storeFileExists( File storeFile )
    {
        try
        {
            return pageCache.getCachedFileSystem().streamFilesRecursive( storeFile ).anyMatch( alwaysTrue() );
        }
        catch ( IOException e )
        {
            return false;
        }
    }

    @Override
    public void mkdirs( File dir ) throws IOException
    {
        pageCache.getCachedFileSystem().mkdirs( dir );
    }

    private static FileHandle storeFileHandle( PageCache pageCache, File storeFile ) throws IOException
    {
        return pageCache.getCachedFileSystem()
                .streamFilesRecursive( storeFile )
                .findFirst()
                .orElseThrow( () -> new NoSuchFileException( storeFile.getPath() ) );
    }
}
