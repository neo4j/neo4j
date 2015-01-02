/**
 * Copyright (c) 2002-2015 "Neo Technology,"
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

import org.neo4j.kernel.impl.nioneo.store.FileSystemAbstraction;

public class LogFiles
{
    private static final String LOG_FNAME_PATTERN = "(active_tx_log)" +
            "|(nioneo_logical\\.log.*)" + // covers current log, active log marker and backups
            "|(tm_tx_log\\..*)";
    private static final String INDEX_LOG_FNAME_PATTERN = "lucene\\.log\\..*";

    private static final class RegexFilenameFilter implements FilenameFilter
    {
        private final Pattern pattern;

        private RegexFilenameFilter( String pattern )
        {
            this.pattern = Pattern.compile( pattern );
        }

        @Override
        public boolean accept( File dir, String name )
        {
            return pattern.matcher( name ).find();
        }
    }

    /**
     * Moves all logical logs of a database from one directory
     * to another. Since it just renames files (the standard way of moving with
     * JDK6) from and to must be on the same disk partition.
     * @param fs 
     * @param fromDirectory The directory that hosts the database and its logs
     * @param toDirectory The directory to move the log files to
     * @throws IOException If any of the move operations fail for any reason.
     */
    public static void move( FileSystemAbstraction fs, File fromDirectory,
            File toDirectory ) throws IOException
    {
        assert fs.isDirectory( fromDirectory );
        assert fs.isDirectory( toDirectory );

        // Move the neo store log files
        FilenameFilter filter = new RegexFilenameFilter( LOG_FNAME_PATTERN );
        for ( File logFile : fs.listFiles( fromDirectory, filter ) )
        {
            StoreFile.moveFile( fs, logFile.getName(), fromDirectory, toDirectory );
        }

        // Move the lucene legacy transaction log files
        fromDirectory = new File( fromDirectory, "index" );
        toDirectory = new File( toDirectory, "index" );

        if ( fromDirectory.exists() )
        {
            toDirectory.mkdirs();

            filter = new RegexFilenameFilter( INDEX_LOG_FNAME_PATTERN );
            for ( File logFile : fs.listFiles( fromDirectory, filter ) )
            {
                StoreFile.moveFile( fs, logFile.getName(), fromDirectory, toDirectory );
            }
        }
    }
}
