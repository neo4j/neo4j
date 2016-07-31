/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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
package org.neo4j.coreedge.core.consensus.log.segmented;

import java.io.File;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.logging.Log;

/**
 * Deals with file names for the RAFT log. The files are named as
 *
 *   raft.log.0
 *   raft.log.1
 *   ...
 *   raft.log.23
 *   ...
 *
 * where the suffix represents the version, which is a strictly monotonic sequence.
 */
public class FileNames
{
    static final String BASE_FILE_NAME = "raft.log.";
    private static final String VERSION_MATCH = "(0|[1-9]\\d*)";

    private final File baseDirectory;
    private final Pattern logFilePattern;

    /**
     * Creates an object useful for managing RAFT log file names.
     *
     * @param baseDirectory The base directory in which the RAFT log files reside.
     */
    public FileNames( File baseDirectory )
    {
        this.baseDirectory = baseDirectory;
        this.logFilePattern = Pattern.compile( BASE_FILE_NAME + VERSION_MATCH );
    }

    /**
     * Creates a file object for the specific version.
     *
     * @param version The version.
     *
     * @return A file for the specific version.
     */
    File getForVersion( long version )
    {
        return new File( baseDirectory, BASE_FILE_NAME + version );
    }

    /**
     * Looks in the base directory for all suitable RAFT log files and returns a sorted map
     * with the version as key and File as value.
     *
     * @param fileSystem The filesystem.
     * @param log The message log.
     *
     * @return The sorted version to file map.
     */
    public SortedMap<Long,File> getAllFiles( FileSystemAbstraction fileSystem, Log log )
    {
        SortedMap<Long,File> versionFileMap = new TreeMap<>();

        for ( File file : fileSystem.listFiles( baseDirectory ) )
        {
            Matcher matcher = logFilePattern.matcher( file.getName() );

            if ( !matcher.matches() )
            {
                log.warn( "Found out of place file: " + file.getName() );
                continue;
            }

            versionFileMap.put( Long.valueOf( matcher.group( 1 ) ), file );
        }

        return versionFileMap;
    }
}
