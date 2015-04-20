/*
 * Copyright (c) 2002-2015 "Neo Technology,"
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
package org.neo4j.kernel.ha;

import java.io.File;
import java.io.FileFilter;
import java.io.IOException;

import org.neo4j.io.fs.FileUtils;

public enum BranchedDataPolicy
{
    keep_all
            {
                @Override
                public void handle( File storeDir ) throws IOException
                {
                    moveAwayDb( storeDir, newBranchedDataDir( storeDir ) );
                }
            },
    keep_last
            {
                @Override
                public void handle( File storeDir ) throws IOException
                {
                    File branchedDataDir = newBranchedDataDir( storeDir );
                    moveAwayDb( storeDir, branchedDataDir );
                    for ( File file : getBranchedDataRootDirectory( storeDir ).listFiles() )
                    {
                        if ( isBranchedDataDirectory( file ) && !file.equals( branchedDataDir ) )
                        {
                            FileUtils.deleteRecursively( file );
                        }
                    }
                }
            },
    keep_none
            {
                @Override
                public void handle( File storeDir ) throws IOException
                {
                    for ( File file : relevantDbFiles( storeDir ) )
                    {
                        FileUtils.deleteRecursively( file );
                    }
                }
            };

    // Branched directories will end up in <dbStoreDir>/branched/<timestamp>/
    static String BRANCH_SUBDIRECTORY = "branched";

    public abstract void handle( File storeDir ) throws IOException;

    protected void moveAwayDb( File storeDir, File branchedDataDir ) throws IOException
    {
        for ( File file : relevantDbFiles( storeDir ) )
        {
            FileUtils.moveFileToDirectory( file, branchedDataDir );
        }
    }

    File newBranchedDataDir( File storeDir )
    {
        File result = getBranchedDataDirectory( storeDir, System.currentTimeMillis() );
        result.mkdirs();
        return result;
    }

    File[] relevantDbFiles( File storeDir )
    {
        if ( !storeDir.exists() )
        {
            return new File[0];
        }

        return storeDir.listFiles( new FileFilter()
        {
            @Override
            public boolean accept( File file )
            {
                return !file.getName().startsWith( "metrics" ) && !file.getName().startsWith( "messages." ) && !isBranchedDataRootDirectory( file );
            }
        } );
    }

    public static boolean isBranchedDataRootDirectory( File directory )
    {
        return directory.isDirectory() && directory.getName().equals( BRANCH_SUBDIRECTORY );
    }

    public static boolean isBranchedDataDirectory( File directory )
    {
        return directory.isDirectory() && directory.getParentFile().getName().equals( BRANCH_SUBDIRECTORY ) &&
                isAllDigits( directory.getName() );
    }

    private static boolean isAllDigits( String string )
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

    public static File getBranchedDataRootDirectory( File storeDir )
    {
        return new File( storeDir, BRANCH_SUBDIRECTORY );
    }

    public static File getBranchedDataDirectory( File storeDir, long timestamp )
    {
        return new File( getBranchedDataRootDirectory( storeDir ), "" + timestamp );
    }

    public static File[] listBranchedDataDirectories( File storeDir )
    {
        return getBranchedDataRootDirectory( storeDir ).listFiles( new FileFilter()
        {
            @Override
            public boolean accept( File directory )
            {
                return isBranchedDataDirectory( directory );
            }
        } );
    }
}
