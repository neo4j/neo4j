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
package org.neo4j.kernel.impl.storemigration;

import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.util.regex.Pattern;

import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.impl.transaction.log.PhysicalLogFile;

import static java.util.regex.Pattern.compile;

public class LogFiles
{
    public static final FilenameFilter FILENAME_FILTER = new LogicalLogFilenameFilter();

    public static final class LogicalLogFilenameFilter implements FilenameFilter
    {
        private static final Pattern LOG_FILENAME_PATTERN = compile(
                PhysicalLogFile.REGEX_DEFAULT_NAME + PhysicalLogFile.REGEX_DEFAULT_VERSION_SUFFIX + ".*"
        );

        @Override
        public boolean accept( File dir, String name )
        {
            return LOG_FILENAME_PATTERN.matcher( name ).matches();
        }
    }

    /**
     * Moves all logical logs of a database from one directory
     * to another. Since it just renames files (the standard way of moving with
     * JDK6) from and to must be on the same disk partition.
     *
     * @param fs            The host file system
     * @param fromDirectory The directory that hosts the database and its logs
     * @param toDirectory   The directory to move the log files to
     * @throws IOException If any of the move operations fail for any reason.
     */
    public static void move( FileSystemAbstraction fs, File fromDirectory, File toDirectory ) throws IOException
    {
        assert fs.isDirectory( fromDirectory );
        assert fs.isDirectory( toDirectory );

        File[] logFiles = fs.listFiles( fromDirectory, FILENAME_FILTER );
        for ( File logFile : logFiles )
        {
            FileOperation.MOVE.perform( fs, logFile.getName(), fromDirectory, false, toDirectory, false );
        }
    }

}
