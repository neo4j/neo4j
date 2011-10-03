/**
 * Copyright (c) 2002-2011 "Neo Technology,"
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
package org.neo4j.kernel.impl.storemigration;

import java.io.File;
import java.io.IOException;

import org.neo4j.kernel.impl.util.FileUtils;

public class LogFiles
{
    public static void copy( File fromDirectory, File toDirectory ) throws IOException
    {
        for ( File file : fromDirectory.listFiles() )
        {
            if ( recognisedAsLogFile( file ) )
            {
                FileUtils.copyFile( file, new File( toDirectory, file.getName() ) );
            }
        }
    }

    private static boolean recognisedAsLogFile( File file )
    {
        String fileName = file.getName();
        String[] logFileMatchers = {
                "active_tx_log",
                "messages\\.log",
                "nioneo_logical\\.log\\.active",
                "tm_tx_log\\..*"
        };
        for ( String matcher : logFileMatchers )
        {
            if ( fileName.matches( matcher ) )
            {
                return true;
            }
        }
        return false;
    }
}
