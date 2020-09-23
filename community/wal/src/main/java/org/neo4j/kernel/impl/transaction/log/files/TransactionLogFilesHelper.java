/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
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
package org.neo4j.kernel.impl.transaction.log.files;

import java.nio.file.DirectoryStream;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.function.Predicate;
import java.util.regex.Pattern;

import org.neo4j.io.fs.FileSystemAbstraction;

import static java.util.Objects.requireNonNull;
import static java.util.regex.Pattern.compile;
import static java.util.regex.Pattern.quote;

public class TransactionLogFilesHelper
{
    public static final String DEFAULT_NAME = "neostore.transaction.db";
    public static final String CHECKPOINT_FILE_PREFIX = "checkpoint";
    static final DirectoryStream.Filter<Path> DEFAULT_FILENAME_FILTER = new LogicalLogFilenameFilter( quote( DEFAULT_NAME ), quote( CHECKPOINT_FILE_PREFIX ) );
    public static final Predicate<String> DEFAULT_FILENAME_PREDICATE = file -> file.startsWith( DEFAULT_NAME ) || file.startsWith( CHECKPOINT_FILE_PREFIX );

    private static final String VERSION_SUFFIX = ".";
    private static final String REGEX_VERSION_SUFFIX = "\\.";
    private static final Path[] EMPTY_FILES_ARRAY = {};

    private final Path logBaseName;
    private final FileSystemAbstraction fileSystem;
    private final DirectoryStream.Filter<Path> filenameFilter;

    public TransactionLogFilesHelper( FileSystemAbstraction fileSystem, Path directory )
    {
        this( fileSystem, directory, DEFAULT_NAME );
    }

    public TransactionLogFilesHelper( FileSystemAbstraction fileSystem, Path directory, String name )
    {
        this.fileSystem = fileSystem;
        this.logBaseName = directory.resolve( name );
        this.filenameFilter = new LogicalLogFilenameFilter( quote( name ) );
    }

    public Path getLogFileForVersion( long version )
    {
        return Path.of( logBaseName.toAbsolutePath().toString() + VERSION_SUFFIX + version );
    }

    public long getLogVersion( Path historyLogFile )
    {
        String historyLogFilename = historyLogFile.getFileName().toString();
        int index = historyLogFilename.lastIndexOf( VERSION_SUFFIX );
        if ( index == -1 )
        {
            throw new RuntimeException( "Invalid log file '" + historyLogFilename + "'" );
        }
        return Long.parseLong( historyLogFilename.substring( index + VERSION_SUFFIX.length() ) );
    }

    DirectoryStream.Filter<Path> getLogFilenameFilter()
    {
        return filenameFilter;
    }

    public Path[] getMatchedFiles()
    {
        Path[] files = fileSystem.listFiles( logBaseName.getParent(), getLogFilenameFilter() );
        if ( files.length == 0 )
        {
            return EMPTY_FILES_ARRAY;
        }
        return files;
    }

    public void accept( LogVersionVisitor visitor )
    {
        for ( Path file : getMatchedFiles() )
        {
            visitor.visit( file, getLogVersion( file ) );
        }
    }

    private static final class LogicalLogFilenameFilter implements DirectoryStream.Filter<Path>
    {
        private final Pattern[] patterns;

        LogicalLogFilenameFilter( String... logFileNameBase )
        {
            requireNonNull( logFileNameBase );
            patterns = Arrays.stream( logFileNameBase ).map( name -> compile( name + REGEX_VERSION_SUFFIX + ".*" ) ).toArray( Pattern[]::new );
        }

        @Override
        public boolean accept( Path entry )
        {
            for ( Pattern pattern : patterns )
            {
                if ( pattern.matcher( entry.getFileName().toString() ).matches() )
                {
                    return true;
                }
            }
            return false;
        }
    }
}
