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
package org.neo4j.backup.impl;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.stream.Stream;

import org.neo4j.com.storecopy.FileMoveAction;
import org.neo4j.com.storecopy.FileMoveProvider;
import org.neo4j.commandline.admin.CommandFailed;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.test.rule.TestDirectory;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class BackupCopyServiceTest
{
    private FileMoveProvider fileMoveProvider;

    @Rule
    public TestDirectory testDirectory = TestDirectory.testDirectory();

    BackupCopyService subject;

    @Before
    public void setup()
    {
        PageCache pageCache = mock( PageCache.class );
        fileMoveProvider = mock( FileMoveProvider.class );
        FileSystemAbstraction fs = mock( FileSystemAbstraction.class );
        subject = new BackupCopyService( fs, pageCache, fileMoveProvider );
    }

    @Test
    public void logicForMovingBackupsIsDelegatedToFileMovePropagator() throws IOException
    {
        // given
        Path parentDirectory = testDirectory.directory( "parent" ).toPath();
        Path oldLocation = parentDirectory.resolve( "oldLocation" );
        Files.createDirectories( oldLocation );
        Path newLocation = parentDirectory.resolve( "newLocation" );

        // and
        FileMoveAction fileOneMoveAction = mock( FileMoveAction.class );
        FileMoveAction fileTwoMoveAction = mock( FileMoveAction.class );
        when( fileMoveProvider.traverseForMoving( any() ) ).thenReturn( Stream.of( fileOneMoveAction, fileTwoMoveAction ) );

        // when
        subject.moveBackupLocation( oldLocation, newLocation );

        // then file move propagator was requested with correct source and baseDirectory
        verify( fileMoveProvider ).traverseForMoving( oldLocation.toFile() );

        // and files were moved to correct target directory
        verify( fileOneMoveAction ).move( newLocation.toFile() );
        verify( fileTwoMoveAction ).move( newLocation.toFile() );
    }
}
