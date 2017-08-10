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
package org.neo4j.kernel.impl.index;

import java.io.File;
import java.io.IOException;
import java.nio.file.NoSuchFileException;

import org.neo4j.index.internal.gbptree.GBPTree;
import org.neo4j.io.fs.FileHandle;
import org.neo4j.io.pagecache.PageCache;

import static org.neo4j.function.Predicates.alwaysTrue;

/**
 * Utilities for common operations around a {@link GBPTree}.
 */
public class GBPTreeUtil
{
    /**
     * Deletes store file backing a {@link GBPTree}.
     *
     * @param pageCache {@link PageCache} which manages the file.
     * @param storeFile the {@link File} to delete.
     * @throws NoSuchFileException if the {@code storeFile} doesn't exist according to the {@code pageCache}.
     * @throws IOException on failure to delete existing {@code storeFile}.
     */
    public static void delete( PageCache pageCache, File storeFile ) throws IOException
    {
        FileHandle fileHandle = storeFileHandle( pageCache, storeFile );
        fileHandle.delete();
    }

    /**
     * Deletes store file backing a {@link GBPTree}, if it exists according to the {@code pageCache}.
     *
     * @param pageCache {@link PageCache} which manages the file.
     * @param storeFile the {@link File} to delete.
     * @throws IOException on failure to delete existing {@code storeFile}.
     */
    public static void deleteIfPresent( PageCache pageCache, File storeFile ) throws IOException
    {
        try
        {
            delete( pageCache, storeFile );
        }
        catch ( NoSuchFileException e )
        {
            // File does not exist, we don't need to delete
        }
    }

    /**
     * Checks whether or not {@code storeFile} exists according to {@code pageCache}.
     *
     * @param pageCache {@link PageCache} which manages the file.
     * @param storeFile the {@link File} to check for existence.
     * @return {@code true} if {@code storeFile} exists according to {@code pageCache}, otherwise {@code false}.
     */
    public static boolean storeFileExists( PageCache pageCache, File storeFile )
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

    private static FileHandle storeFileHandle( PageCache pageCache, File storeFile ) throws IOException
    {
        return pageCache.getCachedFileSystem()
                .streamFilesRecursive( storeFile )
                .findFirst()
                .orElseThrow( () -> new NoSuchFileException( storeFile.getPath() ) );
    }
}
