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
import org.mockito.InOrder;
import org.mockito.Mockito;

import java.io.IOException;
import java.nio.file.Path;

import org.neo4j.commandline.admin.OutsideWorld;
import org.neo4j.consistency.checking.full.ConsistencyFlags;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.util.OptionalHostnamePort;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;
import org.neo4j.test.rule.TestDirectory;

import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class BackupStrategyWrapperTest
{
    @Rule
    public TestDirectory testDirectory = TestDirectory.testDirectory();

    private BackupStrategy backupStrategyImplementation = mock( BackupStrategy.class );
    private OutsideWorld outsideWorld = mock( OutsideWorld.class );
    private BackupCopyService backupCopyService = mock( BackupCopyService.class );

    private BackupStrategyWrapper subject;

    private OnlineBackupContext onlineBackupContext;

    private FileSystemAbstraction fileSystemAbstraction = mock( FileSystemAbstraction.class );
    private Path desiredBackupLocation;
    private Path reportDir;
    private Path availableFreshBackupLocation = mock( Path.class, "Path<availableFreshBackupLocation>" );
    private Path availableOldBackupLocation = mock( Path.class, "Path<availableOldBackupLocation>" );
    private OnlineBackupRequiredArguments requiredArguments;
    private Config config = mock( Config.class );
    private OptionalHostnamePort userProvidedAddress = new OptionalHostnamePort( (String) null, null, null );
    private Fallible<BackupStageOutcome> SUCCESS = new Fallible<>( BackupStageOutcome.SUCCESS, null );
    private Fallible<BackupStageOutcome> FAILURE = new Fallible<>( BackupStageOutcome.FAILURE, null );
    private PageCache pageCache = mock( PageCache.class );
    private BackupRecoveryService backupRecoveryService = mock( BackupRecoveryService.class );
    private LogProvider logProvider = mock( LogProvider.class );
    private Log log = mock( Log.class );

    @Before
    public void setup()
    {
        desiredBackupLocation = testDirectory.directory( "desiredBackupLocation" ).toPath();
        reportDir = testDirectory.directory( "reportDir" ).toPath();

        when( outsideWorld.fileSystem() ).thenReturn( fileSystemAbstraction );
        when( backupCopyService.findAnAvailableLocationForNewFullBackup( any() ) ).thenReturn( availableFreshBackupLocation );
        when( backupCopyService.findNewBackupLocationForBrokenExisting( any() ) ).thenReturn( availableOldBackupLocation );
        when( backupStrategyImplementation.performFullBackup( any(), any(), any() ) ).thenReturn( SUCCESS );
        when( logProvider.getLog( (Class) any() ) ).thenReturn( log );

        subject = new BackupStrategyWrapper( backupStrategyImplementation, backupCopyService, pageCache, config, backupRecoveryService, logProvider );
    }

    @Test
    public void lifecycleIsRun() throws Throwable
    {
        // given
        onlineBackupContext = new OnlineBackupContext( requiredArguments( true ), config, consistencyFlags() );

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
        requiredArguments = requiredArguments( true );
        onlineBackupContext = new OnlineBackupContext( requiredArguments, config, consistencyFlags() );

        // and
        when( backupCopyService.backupExists( any() ) ).thenReturn( false );

        // when
        subject.doBackup( onlineBackupContext );

        // then
        verify( backupStrategyImplementation ).performFullBackup( any(), any(), any() );
    }

    @Test
    public void fullBackupIsIgnoredIfNoOtherBackupAndNotFallback()
    {
        // given there is an existing backup
        when( backupCopyService.backupExists( any() ) ).thenReturn( false );

        // and we don't want to fallback to full backups
        requiredArguments = requiredArguments( false );
        onlineBackupContext = new OnlineBackupContext( requiredArguments, config, consistencyFlags() );

        // and incremental backup fails because it's a different store
        when( backupStrategyImplementation.performIncrementalBackup( any(), any(), any() ) ).thenReturn( FAILURE );

        // when
        subject.doBackup( onlineBackupContext );

        // then full backup wasnt performed
        verify( backupStrategyImplementation, never() ).performFullBackup( any(), any(), any() );
    }

    @Test
    public void fullBackupIsNotPerformedWhenAnIncrementalBackupIsSuccessful()
    {
        // given
        when( backupCopyService.backupExists( any() ) ).thenReturn( true );
        when( backupStrategyImplementation.performIncrementalBackup( any(), any(), any() ) ).thenReturn( SUCCESS );

        // and
        requiredArguments = requiredArguments( true );
        onlineBackupContext = new OnlineBackupContext( requiredArguments, config, consistencyFlags() );

        // when
        subject.doBackup( onlineBackupContext );

        // then
        verify( backupStrategyImplementation, never() ).performFullBackup( desiredBackupLocation, config, userProvidedAddress );
    }

    @Test
    public void failedIncrementalFallsBackToFullWhenOptionSet()
    {
        // given conditions for incremental exist
        when( backupCopyService.backupExists( any() ) ).thenReturn( true );
        requiredArguments = requiredArguments( true );
        onlineBackupContext = new OnlineBackupContext( requiredArguments, config, consistencyFlags() );

        // and incremental fails
        when( backupStrategyImplementation.performIncrementalBackup( any(), any(), any() ) ).thenReturn(
                new Fallible<>( BackupStageOutcome.FAILURE, null ) );

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
        when( backupStrategyImplementation.performIncrementalBackup( any(), any(), any() ) ).thenReturn(
                new Fallible<>( BackupStageOutcome.FAILURE, null ) );

        // and
        requiredArguments = requiredArguments( false );
        onlineBackupContext = new OnlineBackupContext( requiredArguments, config, consistencyFlags() );

        // when
        subject.doBackup( onlineBackupContext );

        // then
        verify( backupStrategyImplementation, never() ).performFullBackup( any(), any(), any() );
    }

    @Test
    public void failedBackupsDontMoveExisting() throws IOException
    {
        // given a backup already exists
        when( backupCopyService.backupExists( any() ) ).thenReturn( true );

        // and fallback to full is true
        requiredArguments = requiredArguments( true );
        onlineBackupContext = new OnlineBackupContext( requiredArguments, config, consistencyFlags() );

        // and an incremental backup fails
        when( backupStrategyImplementation.performIncrementalBackup( any(), any(), any() ) ).thenReturn(
                new Fallible<>( BackupStageOutcome.FAILURE, null ) );

        // and full backup fails
        when( backupStrategyImplementation.performFullBackup( any(), any(), any() ) ).thenReturn(
                new Fallible<>( BackupStageOutcome.FAILURE, null ) );

        // when backup is performed
        subject.doBackup( onlineBackupContext );

        // then existing backup hasn't moved
        verify( backupStrategyImplementation ).performFullBackup( any(), any(), any() );
        verify( backupCopyService, never() ).moveBackupLocation( any(), any() );
    }

    @Test
    public void successfulFullBackupsMoveExistingBackup() throws IOException
    {
        // given backup exists
        desiredBackupLocation = testDirectory.directory( "some-preexisting-backup" ).toPath();
        when( backupCopyService.backupExists( desiredBackupLocation ) ).thenReturn( true );

        // and fallback to full flag has been set
        requiredArguments = requiredArguments( true );
        onlineBackupContext = new OnlineBackupContext( requiredArguments, config, consistencyFlags() );

        // and a new location for the existing backup is found
        Path newLocationForExistingBackup = testDirectory.directory( "new-backup-location" ).toPath();
        when( backupCopyService.findNewBackupLocationForBrokenExisting( desiredBackupLocation ) )
                .thenReturn( newLocationForExistingBackup );

        // and there is a generated location for where to store a new full backup so the original is not destroyed
        Path temporaryFullBackupLocation = testDirectory.directory( "temporary-full-backup" ).toPath();
        when( backupCopyService.findAnAvailableLocationForNewFullBackup( desiredBackupLocation ) )
                .thenReturn( temporaryFullBackupLocation );

        // and incremental fails
        when( backupStrategyImplementation.performIncrementalBackup( any(), any(), any() ) ).thenReturn(
                new Fallible<>( BackupStageOutcome.FAILURE, null ) );

        // and full passes
        when( backupStrategyImplementation.performFullBackup( any(), any(), any() ) ).thenReturn(
                new Fallible<>( BackupStageOutcome.SUCCESS, null ) );

        // when
        Fallible<BackupStrategyOutcome> state = subject.doBackup( onlineBackupContext );

        // then original existing backup is moved to err directory
        verify( backupCopyService ).moveBackupLocation( eq( desiredBackupLocation ), eq( newLocationForExistingBackup ) );

        // and new successful backup is renamed to original expected name
        verify( backupCopyService ).moveBackupLocation( eq( temporaryFullBackupLocation ), eq( desiredBackupLocation ) );

        // and backup was successful
        assertEquals( BackupStrategyOutcome.SUCCESS, state.getState() );
    }

    @Test
    public void failureDuringMoveCausesAbsoluteFailure() throws IOException
    {
        // given moves fail
        doThrow( IOException.class ).when( backupCopyService ).moveBackupLocation( any(), any() );

        // and fallback to full
        requiredArguments = requiredArguments( true );
        onlineBackupContext = new OnlineBackupContext( requiredArguments, config, consistencyFlags() );

        // and backup exists
        when( backupCopyService.backupExists( any() ) ).thenReturn( true );

        // and incremental fails
        when( backupStrategyImplementation.performIncrementalBackup( any(), any(), any() ) ).thenReturn(
                new Fallible<>( BackupStageOutcome.FAILURE, null ) );

        // and full passes
        when( backupStrategyImplementation.performFullBackup( any(), any(), any() ) ).thenReturn(
                new Fallible<>( BackupStageOutcome.SUCCESS, null ) );

        // when
        Fallible<BackupStrategyOutcome> state = subject.doBackup( onlineBackupContext );

        // then result was catastrophic and contained reason
        assertEquals( BackupStrategyOutcome.ABSOLUTE_FAILURE, state.getState() );
        assertEquals( IOException.class, state.getCause().get().getClass() );

        // and full backup was definitely performed
        verify( backupStrategyImplementation ).performFullBackup( any(), any(), any() );
    }

    @Test
    public void performingFullBackupInvokesRecovery()
    {
        // given full backup flag is set
        requiredArguments = requiredArguments( true );
        onlineBackupContext = new OnlineBackupContext( requiredArguments, config, consistencyFlags() );

        // when
        subject.doBackup( onlineBackupContext );

        // then
        verify( backupRecoveryService ).recoverWithDatabase( any(), any(), any() );
    }

    @Test
    public void performingIncrementalBackupInvokesRecovery()
    {
        // given backup exists
        when( backupCopyService.backupExists( any() ) ).thenReturn( true );
        requiredArguments = requiredArguments( true );
        onlineBackupContext = new OnlineBackupContext( requiredArguments, config, consistencyFlags() );

        // and incremental backups are successful
        when( backupStrategyImplementation.performIncrementalBackup( any(), any(), any() ) ).thenReturn( SUCCESS );

        // when
        subject.doBackup( onlineBackupContext );

        // then
        verify( backupRecoveryService ).recoverWithDatabase( any(), any(), any() );
    }

    @Test
    public void successfulBackupsAreRecovered()
    {
        // given
        fallbackToFullPasses();
        onlineBackupContext = new OnlineBackupContext( requiredArguments, config, consistencyFlags() );

        // when
        subject.doBackup( onlineBackupContext );

        // then
        verify( backupRecoveryService ).recoverWithDatabase( any(), any(), any() );
    }

    @Test
    public void unsuccessfulBackupsAreNotRecovered()
    {
        // given
        bothBackupsFail();
        onlineBackupContext = new OnlineBackupContext( requiredArguments, config, consistencyFlags() );

        // when
        subject.doBackup( onlineBackupContext );

        // then
        verify( backupRecoveryService, never() ).recoverWithDatabase( any(), any(), any() );
    }

    @Test
    public void successfulFullBackupsAreRecoveredEvenIfNoBackupExisted()
    {
        // given a backup exists
        when( backupCopyService.backupExists( desiredBackupLocation ) ).thenReturn( false );
        when( backupCopyService.findAnAvailableLocationForNewFullBackup( desiredBackupLocation ) )
                .thenReturn( desiredBackupLocation );

        // and
        fallbackToFullPasses();
        onlineBackupContext = new OnlineBackupContext( requiredArguments, config, consistencyFlags() );

        // when
        subject.doBackup( onlineBackupContext );

        // then
        verify( backupRecoveryService ).recoverWithDatabase( any(), any(), any() );
    }

    @Test
    public void recoveryIsPerformedBeforeRename() throws IOException
    {
        // given
        fallbackToFullPasses();
        onlineBackupContext = new OnlineBackupContext( requiredArguments, config, consistencyFlags() );

        // when
        subject.doBackup( onlineBackupContext );

        // then
        InOrder recoveryBeforeRenameOrder = Mockito.inOrder( backupRecoveryService, backupCopyService );
        recoveryBeforeRenameOrder.verify( backupRecoveryService ).recoverWithDatabase( eq( availableFreshBackupLocation ), any(), any() );
        recoveryBeforeRenameOrder.verify( backupCopyService ).moveBackupLocation( eq( availableFreshBackupLocation ), eq( desiredBackupLocation ) );
    }

    @Test
    public void logsAreClearedAfterIncrementalBackup() throws IOException
    {
        // given backup exists
        when( backupCopyService.backupExists( any() ) ).thenReturn( true );

        // and
        incrementalBackupIsSuccessful( true );

        // and
        requiredArguments = requiredArguments(false);
        onlineBackupContext = new OnlineBackupContext( requiredArguments, config, consistencyFlags() );

        // when
        subject.doBackup( onlineBackupContext );

        // then
        verify( backupCopyService ).clearIdFiles( any() );
    }

    @Test
    public void logsAreNotClearedWhenIncrementalNotSuccessful() throws IOException
    {
        // given backup exists
        when( backupCopyService.backupExists( any() ) ).thenReturn( true );

        // and incremental is not successful
        incrementalBackupIsSuccessful( false );

        // and
        requiredArguments = requiredArguments(false);
        onlineBackupContext = new OnlineBackupContext( requiredArguments, config, consistencyFlags() );

        // when backups are performed
        subject.doBackup( onlineBackupContext );

        // then do not
        verify( backupCopyService, never() ).clearIdFiles( any() );
    }

    @Test
    public void logsAreClearedWhenFullBackupIsSuccessful() throws IOException
    {
        // given a backup doesn't exist
        when( backupCopyService.backupExists( any() ) ).thenReturn( false );

        // and
        fallbackToFullPasses();
        onlineBackupContext = new OnlineBackupContext( requiredArguments, config, consistencyFlags() );

        // when
        subject.doBackup( onlineBackupContext );

        // then
        verify( backupCopyService ).clearIdFiles( any() );
    }

    @Test
    public void logsAreNotClearedWhenFullBackupIsNotSuccessful() throws IOException
    {
        // given a backup doesn't exist
        when( backupCopyService.backupExists( any() ) ).thenReturn( false );

        // and
        bothBackupsFail();
        onlineBackupContext = new OnlineBackupContext( requiredArguments, config, consistencyFlags() );

        // when
        subject.doBackup( onlineBackupContext );

        // then
        verify( backupCopyService, never() ).clearIdFiles( any() );
    }

    @Test
    public void logsWhenIncrementalFailsAndFallbackToFull()
    {
        // given backup exists
        when( backupCopyService.backupExists( any() ) ).thenReturn( false );

        // and fallback to full
        fallbackToFullPasses();
        onlineBackupContext = new OnlineBackupContext( requiredArguments, config, consistencyFlags() );

        // when
        subject.doBackup( onlineBackupContext );

        // then
        verify( log ).info( "Previous backup not found, a new full backup will be performed." );
    }

    // ====================================================================================================

    private void incrementalBackupIsSuccessful( boolean isSuccessful )
    {
        if ( isSuccessful )
        {
            when( backupStrategyImplementation.performIncrementalBackup( any(), any(), any() ) ).thenReturn(
                    new Fallible<>( BackupStageOutcome.SUCCESS, null ) );
            return;
        }
        when( backupStrategyImplementation.performIncrementalBackup( any(), any(), any() ) ).thenReturn(
                new Fallible<>( BackupStageOutcome.FAILURE, null ) );
    }

    private void bothBackupsFail()
    {
        requiredArguments = requiredArguments( true );
        when( backupStrategyImplementation.performIncrementalBackup( any(), any(), any() ) ).thenReturn( FAILURE );
        when( backupStrategyImplementation.performFullBackup( any(), any(), any() ) ).thenReturn( FAILURE );
    }

    private void fallbackToFullPasses()
    {
        requiredArguments = requiredArguments( true );
        when( backupStrategyImplementation.performIncrementalBackup( any(), any(), any() ) ).thenReturn( FAILURE );
        when( backupStrategyImplementation.performFullBackup( any(), any(), any() ) ).thenReturn( SUCCESS );
    }

    private OnlineBackupRequiredArguments requiredArguments( boolean fallbackToFull )
    {
        return new OnlineBackupRequiredArguments( userProvidedAddress, desiredBackupLocation.getParent(), desiredBackupLocation.getFileName().toString(),
                SelectedBackupProtocol.ANY, fallbackToFull, true, 1000, reportDir );
    }

    private static ConsistencyFlags consistencyFlags()
    {
        return new ConsistencyFlags( true, true, true, true );
    }
}
