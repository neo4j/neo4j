/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
package org.neo4j.backup;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import org.neo4j.commandline.admin.CommandFailed;
import org.neo4j.commandline.admin.OutsideWorld;

import static java.lang.String.format;

public class BackupCopyService
{
    private static final int MAX_OLD_BACKUPS = 1000;

    private final OutsideWorld outsideWorld;

    public BackupCopyService( OutsideWorld outsideWorld )
    {
        this.outsideWorld = outsideWorld;
    }

    public void moveBackupLocation( File oldLocation, File newLocation ) throws CommandFailed
    {
        try
        {
            outsideWorld.fileSystem().renameFile( oldLocation, newLocation );
        }
        catch ( IOException e )
        {
            throw new CommandFailed( "Failed to move old backup out of the way: " + e.getMessage(), e );
        }
    }

    boolean backupExists( File destination )
    {
        File[] listFiles = outsideWorld.fileSystem().listFiles( destination );
        return listFiles != null && listFiles.length > 0;
    }

    File findNewBackupLocationForBrokenExisting( File existingBackup )
    {
        return findAnAvailableBackupLocation( existingBackup, "%s.err.%d" );
    }

    File findAnAvailableLocationForNewFullBackup( File desiredBackupLocation )
    {
        return findAnAvailableBackupLocation( desiredBackupLocation, "%s.temp.%d" );
    }

    private File findAnAvailableBackupLocation( File file, String pattern )
    {
        if ( backupExists( file ) )
        {
            // find alternative name
            final AtomicLong counter = new AtomicLong( 0 );
            Consumer<File> countNumberOfFilesProcessedForPotentialErrorMessage = generatedBackupFile -> counter.getAndIncrement();

            return availableAlternativeNames( file, pattern )
                    .peek( countNumberOfFilesProcessedForPotentialErrorMessage )
                    .filter( f -> !backupExists(f) )
                    .findFirst()
                    .orElseThrow( () -> new RuntimeException(
                            String.format( "Unable to find a free backup location for the provided %s. Number of iterations %d", file, counter.get() ) ) );
        }
        return file;
    }

    private static Stream<File> availableAlternativeNames( File originalBackupDirectory, String pattern )
    {
        return IntStream.range( 0, MAX_OLD_BACKUPS )
                .mapToObj( iteration -> alteredBackupDirectoryName( pattern, originalBackupDirectory, iteration ) );
    }

    private static File alteredBackupDirectoryName( String pattern, File directory, int iteration )
    {
        return directory
                .toPath()
                .resolveSibling( format( pattern, directory.getName(), iteration ) )
                .toFile();
    }
}
