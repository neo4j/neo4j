/*
 * Copyright (c) 2002-2019 "Neo4j,"
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
package org.neo4j.causalclustering.core.consensus.log.segmented;

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
