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

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.io.File;
import java.io.IOException;
import java.nio.file.StandardCopyOption;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Stream;

import org.neo4j.com.storecopy.FileMoveAction;
import org.neo4j.com.storecopy.FileMovePropagator;
import org.neo4j.commandline.admin.CommandFailed;
import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.test.rule.TestDirectory;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyVararg;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class BackupCopyServiceTest
{
    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    private static final FileSystemAbstraction fileSystemAbstraction = new DefaultFileSystemAbstraction();

    @Rule
    public TestDirectory testDirectory = TestDirectory.testDirectory(fileSystemAbstraction);

    private PageCache pageCache;
    private FileMovePropagator fileMovePropagator;

    private BackupCopyService subject;

    @Before
    public void setup()
    {
        pageCache = mock( PageCache.class );
        fileMovePropagator = mock( FileMovePropagator.class );

        when( pageCache.getCachedFileSystem() ).thenReturn( fileSystemAbstraction );

        subject = new BackupCopyService( pageCache, fileMovePropagator );
    }

    @Test
    public void backupExistsWhenAtLeastOneFile() throws IOException
    {
        // given destination has at least one file
        File destination = testDirectory.directory( "aDirectory" );
        createFile( destination, "aFile" );

        // when
        boolean backupExists = subject.backupExists( destination );

        // then
        assertTrue( backupExists );
    }

    @Test
    public void backupDoesntExistWhenNoFiles()
    {
        // given destination has no contents
        File destination = testDirectory.directory( "aDirectory" );

        // when
        boolean backupExists = subject.backupExists( destination );

        // then
        assertFalse( backupExists );
    }

    @Test
    public void backupRenamesAllIndividualFiles() throws CommandFailed, IOException
    {
        // given a sourceDirectory and targetDirectory
        File sourceDirectory = testDirectory.directory( "sourceDirectory" );
        File targetDirectory = testDirectory.directory( "targetDirectory" );

        // and ...
        FileMoveAction fileMoveActionOne = mock( FileMoveAction.class );
        FileMoveAction fileMoveActionTwo = mock( FileMoveAction.class );
        when( fileMovePropagator.traverseGenerateMoveActions( sourceDirectory, sourceDirectory ) )
                .thenReturn( Stream.of(
                        fileMoveActionOne,
                        fileMoveActionTwo ) );

        // when
        subject.moveBackupLocation( sourceDirectory, targetDirectory );

        // then
        verify( fileMoveActionOne ).move( targetDirectory, StandardCopyOption.REPLACE_EXISTING );
        verify( fileMoveActionTwo ).move( targetDirectory, StandardCopyOption.REPLACE_EXISTING );
    }

    @Test
    public void errorMatchesWhenRenameFails() throws CommandFailed, IOException
    {
        // given conditions for a rename to occur
        File source = testDirectory.file( "sourceFile" );
        File target = testDirectory.file( "targetFile" );
        FileMoveAction fileMoveAction = mock( FileMoveAction.class );

        when( fileMovePropagator.traverseGenerateMoveActions( source, source ) ).thenReturn( Stream.of( fileMoveAction ) );

        // and exception when rename is attempted
        doThrow( new IOException( "bla bla" ) ).when( fileMoveAction ).move( eq( target ), any() );

        // then
        expectedException.expect( CommandFailed.class );
        expectedException.expectMessage( "Failed to move old backup out of the way: bla bla" );

        // when
        subject.moveBackupLocation( source, target );
    }

    @Test
    public void brokenBackupsAreRenamedWithCorrectPattern() throws IOException
    {
        // given
        File aBrokenBackup = testDirectory.directory( "broken-backup" );

        // and name is already taken (by itself)
        createFile( aBrokenBackup, "any-file" );

        // when
        File newBackupLocation = subject.findNewBackupLocationForBrokenExisting( aBrokenBackup );

        // then
        assertEquals( "broken-backup.err.0", newBackupLocation.getName() );
    }

    @Test
    public void newBackupsArePerformedIntoTemporaryDirectory() throws IOException
    {
        // given
        File desiredBackupDirectory = testDirectory.directory( "backup-directory" );

        // and the backup is already taken
        createFile( desiredBackupDirectory, "any file");

        // when
        File temporaryBackupDirectory = subject.findAnAvailableLocationForNewFullBackup( desiredBackupDirectory );

        // then
        assertEquals( "backup-directory.temp.0", temporaryBackupDirectory.getName() );
    }

    private File createFile( File directory, String filename ) throws IOException
    {
        File file = new File( directory, filename );
        file.createNewFile();
        return file;
    }
}
