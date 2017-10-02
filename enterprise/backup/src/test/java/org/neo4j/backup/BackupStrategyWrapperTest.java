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
import org.junit.Test;

import java.io.File;

import org.neo4j.commandline.admin.CommandFailed;
import org.neo4j.commandline.admin.OutsideWorld;
import org.neo4j.helpers.OptionalHostnamePort;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.configuration.Config;

import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class BackupStrategyWrapperTest
{
    private BackupStrategy backupStrategyImplementation = mock( BackupStrategy.class );
    private OutsideWorld outsideWorld = mock( OutsideWorld.class );
    private BackupCopyService backupCopyService = mock(BackupCopyService.class);

    private BackupStrategyWrapper subject;

    private OnlineBackupContext onlineBackupContext = mock( OnlineBackupContext.class );

    private FileSystemAbstraction fileSystemAbstraction = mock( FileSystemAbstraction.class );
    private File backupLocation = mock( File.class );
    private OnlineBackupRequiredArguments requiredArguments = mock( OnlineBackupRequiredArguments.class );
    private Config config = mock( Config.class );
    private OptionalHostnamePort userProvidedAddress = new OptionalHostnamePort( (String) null, null, null );
    private PotentiallyErroneousState<BackupStageOutcome> SUCCESS = new PotentiallyErroneousState<>( BackupStageOutcome.SUCCESS, null );

    @Before
    public void setup()
    {
        when( outsideWorld.fileSystem() ).thenReturn( fileSystemAbstraction );
        when( onlineBackupContext.getResolvedLocationFromName() ).thenReturn( backupLocation );
        when( onlineBackupContext.getRequiredArguments() ).thenReturn( requiredArguments );
        when( backupStrategyImplementation.performFullBackup( any(), any(), any() ) ).thenReturn( SUCCESS );
        subject = new BackupStrategyWrapper( backupStrategyImplementation, backupCopyService );
    }

    @Test
    public void lifecycleIsRun() throws Throwable
    {
        // when
        subject.doBackup( onlineBackupContext );

        // then
        verify( backupStrategyImplementation ).init();
        verify( backupStrategyImplementation ).start();
        verify( backupStrategyImplementation ).stop();
        verify( backupStrategyImplementation ).shutdown();
    }

    @Test
    public void fullBackupIsPerformedWhenNoOtherBackupExists()
    {
        // given
        when( backupCopyService.backupExists( any() ) ).thenReturn( false );

        // when
        subject.doBackup( onlineBackupContext );

        // then
        verify( backupStrategyImplementation ).performFullBackup( any(), any(), any() );
    }

    @Test
    public void fullBackupIsNotPerformedWhenAnIncrementalBackupIsSuccessful()
    {
        // given
        when( backupCopyService.backupExists( any() ) ).thenReturn( true );
        when( backupStrategyImplementation.performIncrementalBackup( any(), any(), any() ) ).thenReturn( SUCCESS );

        // when
        subject.doBackup( onlineBackupContext );

        // then
        verify( backupStrategyImplementation, never() ).performFullBackup( backupLocation, config, userProvidedAddress );
    }

    @Test
    public void failedIncrementalFallsBackToFullWhenOptionSet()
    {
        // given conditions for incremental exist
        when( backupCopyService.backupExists( any()  ) ).thenReturn( true );
        when( requiredArguments.isFallbackToFull() ).thenReturn( true );

        // and incremental fails
        when( backupStrategyImplementation.performIncrementalBackup( any(), any(), any() ) )
                .thenReturn( new PotentiallyErroneousState<>( BackupStageOutcome.FAILURE, null ) );

        // when
        subject.doBackup( onlineBackupContext );

        // then
        verify( backupStrategyImplementation ).performFullBackup( any(), any(), any() );
    }

    @Test
    public void fallbackDoesNotHappenIfNotSpecified()
    {
        // given
        when( backupCopyService.backupExists( any() ) ).thenReturn( true );
        when( requiredArguments.isFallbackToFull() ).thenReturn( false );
        when( backupStrategyImplementation.performIncrementalBackup( any(), any(), any() ) )
                .thenReturn( new PotentiallyErroneousState<>( BackupStageOutcome.FAILURE, null ) );

        // when
        subject.doBackup( onlineBackupContext );

        // then
        verify( backupStrategyImplementation, never() ).performFullBackup( any(), any(), any() );
    }

    @Test
    public void failedBackupsDontMoveExisting() throws CommandFailed
    {
        // given a backup already exists
        when( backupCopyService.backupExists( any() ) ).thenReturn( true );

        // and fallback to full is true
        when( requiredArguments.isFallbackToFull() ).thenReturn( true );

        // and an incremental backup fails
        when( backupStrategyImplementation.performIncrementalBackup( any(), any(), any() ) )
                .thenReturn( new PotentiallyErroneousState<>( BackupStageOutcome.FAILURE, null ) );

        // and full backup fails
        when( backupStrategyImplementation.performFullBackup( any(), any(), any() ))
                .thenReturn( new PotentiallyErroneousState<>( BackupStageOutcome.FAILURE, null ) );

        // when backup is performed
        subject.doBackup( onlineBackupContext );

        // then existing backup hasn't moved
        verify( backupStrategyImplementation ).performFullBackup( any(), any(), any() );
        verify( backupCopyService, never() ).moveBackupLocation( any(), any() );
    }

    @Test
    public void successfulFullBackupsMoveExistingBackup() throws CommandFailed
    {
        // given backup exists
        File desiredBackupLocation = new File( "some-preexisting-backup" );
        when( onlineBackupContext.getResolvedLocationFromName() ).thenReturn( desiredBackupLocation );
        when( backupCopyService.backupExists( desiredBackupLocation ) ).thenReturn( true );

        // and fallback to full flag has been set
        when( requiredArguments.isFallbackToFull() ).thenReturn( true );

        // and a new location for the existing backup is found
        File newLocationForExistingBackup = new File( "new-backup-location" );
        when( backupCopyService.findNewBackupLocationForBrokenExisting( desiredBackupLocation ) ).thenReturn( newLocationForExistingBackup );

        // and there is a generated location for where to store a new full backup so the original is not destroyed
        File temporaryFullBackupLocation = new File( "temporary-full-backup" );
        when( backupCopyService.findAnAvailableLocationForNewFullBackup( desiredBackupLocation ) ).thenReturn( temporaryFullBackupLocation );

        // and incremental fails
        when( backupStrategyImplementation.performIncrementalBackup( any(), any(), any() ) )
                .thenReturn( new PotentiallyErroneousState<>( BackupStageOutcome.FAILURE, null ) );

        // and full passes
        when( backupStrategyImplementation.performFullBackup( any(), any(), any() ) )
                .thenReturn( new PotentiallyErroneousState<>( BackupStageOutcome.SUCCESS, null ) );

        // when
        PotentiallyErroneousState<BackupStrategyOutcome> state = subject.doBackup( onlineBackupContext );

        // then original existing backup is moved to err directory
        verify( backupCopyService ).moveBackupLocation( desiredBackupLocation, newLocationForExistingBackup );

        // and new successful backup is renamed to original expected name
        verify( backupCopyService ).moveBackupLocation( temporaryFullBackupLocation, desiredBackupLocation );

        // and backup was successful
        assertEquals( BackupStrategyOutcome.SUCCESS, state.getState() );
    }

    @Test
    public void failureDuringMoveCausesAbsoluteFailure() throws CommandFailed
    {
        // given moves fail
        doThrow( CommandFailed.class ).when( backupCopyService ).moveBackupLocation( any(), any() );

        // and fallback to full
        when( requiredArguments.isFallbackToFull() ).thenReturn( true );

        // and backup exists
        when( backupCopyService.backupExists( any() )).thenReturn( true );

        // and incremental fails
        when( backupStrategyImplementation.performIncrementalBackup( any(), any(), any() ))
                .thenReturn( new PotentiallyErroneousState<>( BackupStageOutcome.FAILURE, null ) );

        // and full passes
        when( backupStrategyImplementation.performFullBackup( any(), any(), any() ) )
                .thenReturn( new PotentiallyErroneousState<>( BackupStageOutcome.SUCCESS, null ) );

        // when
        PotentiallyErroneousState<BackupStrategyOutcome> state = subject.doBackup( onlineBackupContext );

        // then result was catastrophic and contained reason
        assertEquals( BackupStrategyOutcome.ABSOLUTE_FAILURE, state.getState() );
        assertEquals( CommandFailed.class, state.getCause().get().getClass() );

        // and full backup was definitely performed
        verify( backupStrategyImplementation ).performFullBackup( any(), any(), any() );
    }
}
