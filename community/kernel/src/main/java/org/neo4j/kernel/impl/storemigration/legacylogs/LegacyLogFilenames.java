/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.kernel.impl.storemigration.legacylogs;

import java.io.File;
import java.io.FilenameFilter;

public class LegacyLogFilenames
{
    private static final String legacyLogFilesPrefix = "nioneo_logical.log.v";
    private static final String versionedLegacyLogFilesPattern = "nioneo_logical\\.log\\.v\\d+";
    private static final String[] allLegacyLogFilesPatterns =
            {"active_tx_log", "tm_tx_log\\..*", "nioneo_logical\\.log\\..*"};

    static final FilenameFilter versionedLegacyLogFilesFilter = new FilenameFilter()
    {
        @Override
        public boolean accept( File dir, String name )
        {
            return name.matches( versionedLegacyLogFilesPattern );
        }
    };

    static final FilenameFilter allLegacyLogFilesFilter = new FilenameFilter()
    {
        @Override
        public boolean accept( File dir, String name )
        {
            for ( String pattern : allLegacyLogFilesPatterns )
            {
                if ( name.matches( pattern ) )
                {
                    return true;
                }
            }
            return false;
        }
    };

    static long getLegacyLogVersion( String filename )
    {
        int index = filename.lastIndexOf( ".v" );
        if ( index == -1 )
        {
            throw new RuntimeException( "Invalid log file '" + filename + "'" );
        }
        return Long.parseLong( filename.substring( index + ".v".length() ) );
    }

    static String getLegacyLogFilename( int version )
    {
        return legacyLogFilesPrefix + version;
    }
}
