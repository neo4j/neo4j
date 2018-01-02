/*
 * Copyright (c) 2002-2018 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.com.storecopy;

import org.apache.commons.lang3.StringUtils;

import java.io.File;
import java.io.FileFilter;
import java.io.IOException;
import java.util.function.Consumer;
import java.util.stream.Stream;

import org.neo4j.io.fs.FileUtils;
import org.neo4j.io.fs.FileHandle;
import org.neo4j.io.pagecache.PageCache;

import static org.neo4j.io.fs.FileHandle.HANDLE_DELETE;

public class StoreUtil
{
    // Branched directories will end up in <dbStoreDir>/branched/<timestamp>/
    public static final String BRANCH_SUBDIRECTORY = "branched";
    private static final String[] DONT_MOVE_DIRECTORIES = {"metrics", "logs", "certificates"};
    public static final String TEMP_COPY_DIRECTORY_NAME = "temp-copy";

    private static final FileFilter STORE_FILE_FILTER = file ->
    {
        for ( String directory : DONT_MOVE_DIRECTORIES )
        {
            if ( file.getName().equals( directory ) )
            {
                return false;
            }
        }
        return !isBranchedDataRootDirectory( file ) && !isTemporaryCopy( file );
    };
    private static final FileFilter DEEP_STORE_FILE_FILTER = file ->
    {
        for ( String directory : DONT_MOVE_DIRECTORIES )
        {
            if ( file.getPath().contains( directory ) )
            {
                return false;
            }
        }
        return !isPartOfBranchedDataRootDirectory( file );
    };

    private StoreUtil()
    {
    }

    public static void cleanStoreDir( File storeDir, PageCache pageCache ) throws IOException
    {
        for ( File file : relevantDbFiles( storeDir ) )
        {
            FileUtils.deleteRecursively( file );
        }

        pageCache.getCachedFileSystem().streamFilesRecursive( storeDir )
                .filter( fh -> DEEP_STORE_FILE_FILTER.accept( fh.getFile() ) ).forEach( HANDLE_DELETE );
    }

    public static File newBranchedDataDir( File storeDir )
    {
        File result = getBranchedDataDirectory( storeDir, System.currentTimeMillis() );
        result.mkdirs();
        return result;
    }

    public static void moveAwayDb( File storeDir, File branchedDataDir, PageCache pageCache ) throws IOException
    {
        for ( File file : relevantDbFiles( storeDir ) )
        {
            FileUtils.moveFileToDirectory( file, branchedDataDir );
        }

        moveAwayDbWithPageCache( storeDir, branchedDataDir, pageCache, DEEP_STORE_FILE_FILTER );
    }

    public static void moveAwayDbWithPageCache( File from, File to, PageCache pageCache, FileFilter filter )
            throws IOException
    {
        final Stream<FileHandle> fileHandleStream;
        try
        {
            fileHandleStream = pageCache.getCachedFileSystem().streamFilesRecursive( from );
        }
        catch ( IOException e )
        {
            // Directory does not exist, has possibly been moved with file system previous to this call.
            return;
        }
        final Consumer<FileHandle> handleRename = FileHandle.handleRenameBetweenDirectories( from, to );
        fileHandleStream.filter( fh -> filter.accept( fh.getFile() ) ).forEach( handleRename );
    }

    public static void deleteRecursive( File storeDir, PageCache pageCache ) throws IOException
    {
        FileUtils.deleteRecursively( storeDir );
        pageCache.getCachedFileSystem().streamFilesRecursive( storeDir ).forEach( HANDLE_DELETE );
    }

    public static boolean isBranchedDataDirectory( File file )
    {
        return file.isDirectory() && file.getParentFile().getName().equals( BRANCH_SUBDIRECTORY ) &&
               StringUtils.isNumeric( file.getName() );
    }

    public static File getBranchedDataRootDirectory( File storeDir )
    {
        return new File( storeDir, BRANCH_SUBDIRECTORY );
    }

    public static File getBranchedDataDirectory( File storeDir, long timestamp )
    {
        return new File( getBranchedDataRootDirectory( storeDir ), "" + timestamp );
    }

    public static File[] relevantDbFiles( File storeDir )
    {
        if ( !storeDir.exists() )
        {
            return new File[0];
        }

        return storeDir.listFiles( STORE_FILE_FILTER );
    }

    private static boolean isBranchedDataRootDirectory( File file )
    {
        return file.isDirectory() && BRANCH_SUBDIRECTORY.equals( file.getName() );
    }

    private static boolean isTemporaryCopy( File file )
    {
        return file.isDirectory() && file.getName().equals( TEMP_COPY_DIRECTORY_NAME );
    }

    private static boolean isPartOfBranchedDataRootDirectory( File file )
    {
        return file.getPath().contains( BRANCH_SUBDIRECTORY );
    }
}
