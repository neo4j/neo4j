/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * GNU AFFERO GENERAL PUBLIC LICENSE Version 3
 * (http://www.fsf.org/licensing/licenses/agpl-3.0.html) with the
 * Commons Clause, as found in the associated LICENSE.txt file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * Neo4j object code can be licensed independently from the source
 * under separate terms from the AGPL. Inquiries can be directed to:
 * licensing@neo4j.com
 *
 * More information is also available at:
 * https://neo4j.com/licensing/
 */
package org.neo4j.com.storecopy;

import org.apache.commons.lang3.StringUtils;

import java.io.File;
import java.io.FileFilter;
import java.io.IOException;

import org.neo4j.io.fs.FileUtils;

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

    private StoreUtil()
    {
    }

    public static void cleanStoreDir( File databaseDirectory ) throws IOException
    {
        for ( File file : relevantDbFiles( databaseDirectory ) )
        {
            FileUtils.deleteRecursively( file );
        }
    }

    public static File newBranchedDataDir( File databaseDirectory )
    {
        File result = getBranchedDataDirectory( databaseDirectory, System.currentTimeMillis() );
        result.mkdirs();
        return result;
    }

    public static void moveAwayDb( File databaseDirectory, File branchedDataDir ) throws IOException
    {
        for ( File file : relevantDbFiles( databaseDirectory ) )
        {
            FileUtils.moveFileToDirectory( file, branchedDataDir );
        }
    }

    public static void deleteRecursive( File databaseDirectory ) throws IOException
    {
        FileUtils.deleteRecursively( databaseDirectory );
    }

    public static boolean isBranchedDataDirectory( File file )
    {
        return file.isDirectory() && file.getParentFile().getName().equals( BRANCH_SUBDIRECTORY ) &&
               StringUtils.isNumeric( file.getName() );
    }

    public static File getBranchedDataRootDirectory( File databaseDirectory )
    {
        return new File( databaseDirectory, BRANCH_SUBDIRECTORY );
    }

    public static File getBranchedDataDirectory( File databaseDirectory, long timestamp )
    {
        return new File( getBranchedDataRootDirectory( databaseDirectory ), "" + timestamp );
    }

    public static File[] relevantDbFiles( File databaseDirectory )
    {
        if ( !databaseDirectory.exists() )
        {
            return new File[0];
        }

        return databaseDirectory.listFiles( STORE_FILE_FILTER );
    }

    private static boolean isBranchedDataRootDirectory( File file )
    {
        return file.isDirectory() && BRANCH_SUBDIRECTORY.equals( file.getName() );
    }

    private static boolean isTemporaryCopy( File file )
    {
        return file.isDirectory() && file.getName().equals( TEMP_COPY_DIRECTORY_NAME );
    }

}
