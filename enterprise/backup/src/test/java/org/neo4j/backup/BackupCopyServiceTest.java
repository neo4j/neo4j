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

import java.io.File;
import java.io.IOException;
import java.util.stream.Stream;

import org.neo4j.com.storecopy.FileMoveAction;
import org.neo4j.com.storecopy.FileMoveProvider;
import org.neo4j.commandline.admin.CommandFailed;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.test.rule.TestDirectory;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class BackupCopyServiceTest
{
    private PageCache pageCache;
    private FileMoveProvider fileMoveProvider;

    @Rule
    public TestDirectory testDirectory = TestDirectory.testDirectory();

    BackupCopyService subject;

    @Before
    public void setup()
    {
        pageCache = mock( PageCache.class );
        fileMoveProvider = mock( FileMoveProvider.class );
        subject = new BackupCopyService( pageCache, fileMoveProvider );
    }

    @Test
    public void logicForMovingBackupsIsDelegatedToFileMovePropagator() throws CommandFailed, IOException
    {
        // given
        File parentDirectory = testDirectory.directory( "parent" );
        File oldLocation = new File( parentDirectory, "oldLocation" );
        oldLocation.mkdir();
        File newLocation = new File( parentDirectory, "newLocation" );

        // and
        FileMoveAction fileOneMoveAction = mock( FileMoveAction.class );
        FileMoveAction fileTwoMoveAction = mock( FileMoveAction.class );
        when( fileMoveProvider.traverseGenerateMoveActions( any() ) ).thenReturn( Stream.of( fileOneMoveAction, fileTwoMoveAction ) );

        // when
        subject.moveBackupLocation( oldLocation, newLocation );

        // then file move propagator was requested with correct source and baseDirectory
        verify( fileMoveProvider ).traverseGenerateMoveActions( oldLocation );

        // and files were moved to correct target directory
        verify( fileOneMoveAction ).move( newLocation );
        verify( fileTwoMoveAction ).move( newLocation );
    }
}
