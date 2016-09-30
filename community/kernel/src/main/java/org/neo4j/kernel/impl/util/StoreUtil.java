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
package org.neo4j.kernel.impl.util;

import java.io.File;
import java.io.FileFilter;
import java.io.IOException;

import org.neo4j.io.fs.FileUtils;
import org.neo4j.io.pagecache.FileHandle;
import org.neo4j.io.pagecache.PageCache;

public class StoreUtil
{
    // Branched directories will end up in <dbStoreDir>/branched/<timestamp>/
    public static final String BRANCH_SUBDIRECTORY = "branched";
    private static final String[] DONT_MOVE_DIRECTORIES = {"metrics", "logs", "certificates"};

    private static final FileFilter DEEP_STORE_FILE_FILTER = file -> {
        for ( String directory : DONT_MOVE_DIRECTORIES )
        {
            if ( file.getPath().contains( directory ) )
            {
                return false;
            }
        }
        return !isPartOfBranchedDataRootDirectory( file );
    };

    public static void cleanStoreDir( File storeDir, PageCache pageCache ) throws IOException
    {
        for ( File file : relevantDbFiles( storeDir ) )
        {
            FileUtils.deleteRecursively( file );
        }

        final Iterable<FileHandle> handles = pageCache.streamFilesRecursive( storeDir )
                .filter( fh -> DEEP_STORE_FILE_FILTER.accept( fh.getFile() ) )::iterator;
        for ( FileHandle handle : handles )
        {
            handle.delete();
        }
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

        Iterable<FileHandle> handles = pageCache.streamFilesRecursive( storeDir )
                .filter( fh -> DEEP_STORE_FILE_FILTER.accept( fh.getFile() ) )::iterator;
        for ( FileHandle handle : handles )
        {
            final File fileToMove = handle.getFile();
            handle.rename( FileUtils.pathToFileAfterMove( storeDir, branchedDataDir, fileToMove ) );
        }
    }

    public static void deleteRecursive( File storeDir, PageCache pageCache ) throws IOException
    {
        FileUtils.deleteRecursively( storeDir );
        Iterable<FileHandle> handles = pageCache.streamFilesRecursive( storeDir )::iterator;
        for ( FileHandle handle : handles )
        {
            handle.delete();
        }
    }

    public static boolean isBranchedDataDirectory( File file )
    {
        return file.isDirectory() && file.getParentFile().getName().equals( BRANCH_SUBDIRECTORY ) &&
               isNumerical( file.getName() );
    }

    public static File getBranchedDataRootDirectory( File storeDir )
    {
        return new File( storeDir, BRANCH_SUBDIRECTORY );
    }

    public static File getBranchedDataDirectory( File storeDir, long timestamp )
    {
        return new File( getBranchedDataRootDirectory( storeDir ), "" + timestamp );
    }

    private static File[] relevantDbFiles( File storeDir )
    {
        if ( !storeDir.exists() )
        {
            return new File[0];
        }

        return storeDir.listFiles( file -> {
            for ( String directory : DONT_MOVE_DIRECTORIES )
            {
                if ( file.getName().equals( directory ) )
                {
                    return false;
                }
            }
            return !isBranchedDataRootDirectory( file );
        } );
    }

    private static boolean isBranchedDataRootDirectory( File file )
    {
        return file.isDirectory() && file.getName().equals( BRANCH_SUBDIRECTORY );
    }

    private static boolean isPartOfBranchedDataRootDirectory( File file )
    {
        return file.getPath().contains( BRANCH_SUBDIRECTORY );
    }

    private static boolean isNumerical( String string )
    {
        for ( char c : string.toCharArray() )
        {
            if ( !Character.isDigit( c ) )
            {
                return false;
            }
        }
        return true;
    }
}
