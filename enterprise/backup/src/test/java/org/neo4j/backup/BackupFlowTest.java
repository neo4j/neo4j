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

import org.hamcrest.CoreMatchers;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collections;

import org.neo4j.commandline.admin.CommandFailed;
import org.neo4j.commandline.admin.OutsideWorld;
import org.neo4j.consistency.ConsistencyCheckService;
import org.neo4j.consistency.checking.full.ConsistencyFlags;
import org.neo4j.consistency.checking.full.ConsistencyCheckIncompleteException;
import org.neo4j.helpers.progress.ProgressMonitorFactory;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.logging.LogProvider;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.Matchers.containsString;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.neo4j.backup.ExceptionMatchers.exceptionContainsSuppressedThrowable;

public class BackupFlowTest
{
    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    // dependencies
    final ConsistencyCheckService consistencyCheckService = mock( ConsistencyCheckService.class );
    final OutsideWorld outsideWorld = mock( OutsideWorld.class );
    final LogProvider logProvider = mock( LogProvider.class );
    final BackupStrategyWrapper firstStrategy = mock( BackupStrategyWrapper.class );
    final BackupStrategyWrapper secondStrategy = mock( BackupStrategyWrapper.class );

    BackupFlow subject;

    // test method parameter mocks
    final OnlineBackupContext onlineBackupContext = mock( OnlineBackupContext.class );
    final OnlineBackupRequiredArguments requiredArguments = mock( OnlineBackupRequiredArguments.class );

    // mock returns
    private ProgressMonitorFactory progressMonitorFactory = mock( ProgressMonitorFactory.class );
    private Path reportDir = mock( Path.class );
    private ConsistencyCheckService.Result consistencyCheckResult = mock( ConsistencyCheckService.Result.class );

    @Before
    public void setup()
    {
        when( onlineBackupContext.getRequiredArguments() ).thenReturn( requiredArguments );
        when( requiredArguments.getReportDir() ).thenReturn( reportDir );
        subject = new BackupFlow( consistencyCheckService, outsideWorld, logProvider, progressMonitorFactory,
                Arrays.asList( firstStrategy, secondStrategy ) );
    }

    @Test
    public void backupIsValidIfAnySingleStrategyPasses_secondFails() throws CommandFailed
    {
        // given
        when( firstStrategy.doBackup( any() ) ).thenReturn( new PotentiallyErroneousState<>( BackupStrategyOutcome.SUCCESS, null ) );
        when( secondStrategy.doBackup( any() ) ).thenReturn( new PotentiallyErroneousState<>( BackupStrategyOutcome.INCORRECT_STRATEGY, null ) );

        // when
        subject.performBackup( onlineBackupContext );

        // then no exception
    }

    @Test
    public void backupIsValidIfAnySingleStrategyPasses_firstFails() throws CommandFailed
    {
        // given
        when( firstStrategy.doBackup( any() ) ).thenReturn( new PotentiallyErroneousState<>( BackupStrategyOutcome.INCORRECT_STRATEGY, null ) );
        when( secondStrategy.doBackup( any() ) ).thenReturn( new PotentiallyErroneousState<>( BackupStrategyOutcome.SUCCESS, null ) );

        // when
        subject.performBackup( onlineBackupContext );
    }

    @Test
    public void backupIsInvalidIfTheCorrectMethodFailed_firstFails() throws CommandFailed
    {
        // given
        when( firstStrategy.doBackup( any() ) ).thenReturn( new PotentiallyErroneousState<>( BackupStrategyOutcome.CORRECT_STRATEGY_FAILED, null ) );
        when( secondStrategy.doBackup( any() ) ).thenReturn( new PotentiallyErroneousState<>( BackupStrategyOutcome.INCORRECT_STRATEGY, null ) );

        // then
        expectedException.expect( CommandFailed.class );
        expectedException.expectMessage( containsString( "Execution of backup failed" ) );

        // when
        subject.performBackup( onlineBackupContext );
    }

    @Test
    public void backupIsInvalidIfTheCorrectMethodFailed_secondFails() throws CommandFailed
    {
        // given
        when( firstStrategy.doBackup( any() ) ).thenReturn( new PotentiallyErroneousState<>( BackupStrategyOutcome.INCORRECT_STRATEGY, null ) );
        when( secondStrategy.doBackup( any() ) ).thenReturn( new PotentiallyErroneousState<>( BackupStrategyOutcome.CORRECT_STRATEGY_FAILED, null ) );

        // then
        expectedException.expect( CommandFailed.class );
        expectedException.expectMessage( containsString( "Execution of backup failed" ) );

        // when
        subject.performBackup( onlineBackupContext );
    }

    @Test
    public void backupFailsIfAllStrategiesAreIncorrect() throws CommandFailed
    {
        // given
        when( firstStrategy.doBackup( any() ) ).thenReturn( new PotentiallyErroneousState<>( BackupStrategyOutcome.INCORRECT_STRATEGY, null ) );
        when( secondStrategy.doBackup( any() ) ).thenReturn( new PotentiallyErroneousState<>( BackupStrategyOutcome.INCORRECT_STRATEGY, null ) );

        // then
        expectedException.expect( CommandFailed.class );
        expectedException.expectMessage( equalTo( "Failed to run a backup using the available strategies." ) );

        // when
        subject.performBackup( onlineBackupContext );
    }

    @Test
    public void consistencyCheckIsRunIfSpecified() throws CommandFailed, IOException, ConsistencyCheckIncompleteException
    {
        // given
        anyStrategyPasses();
        when( requiredArguments.isDoConsistencyCheck() ).thenReturn( true );
        when( consistencyCheckService.runFullConsistencyCheck( any(), any(),
                eq(progressMonitorFactory), any( LogProvider.class ), any( FileSystemAbstraction.class),
                eq(false), any(File.class), any( ConsistencyFlags.class ) ) )
                .thenReturn( consistencyCheckResult );
        when( consistencyCheckResult.isSuccessful() ).thenReturn( true );

        // when
        subject.performBackup( onlineBackupContext );

        // then
        verify( consistencyCheckService ).runFullConsistencyCheck( any(), any(), any(), any(), any(), eq( false ), any(), any( ConsistencyFlags.class ) );
    }

    @Test
    public void consistencyCheckIsNotRunIfNotSpecified() throws CommandFailed, IOException, ConsistencyCheckIncompleteException
    {
        // given
        anyStrategyPasses();
        when( requiredArguments.isDoConsistencyCheck() ).thenReturn( false );

        // when
        subject.performBackup( onlineBackupContext );

        // then
        verify( consistencyCheckService, never() ).runFullConsistencyCheck( any(), any(), any(), any(), any(), eq( false ), any(),
                any( ConsistencyFlags.class ) );
    }

    @Test
    public void allFailureCausesAreCollectedAndAttachedToCommandFailedException() throws CommandFailed
    {
        // given expected causes for failure
        RuntimeException firstCause = new RuntimeException( "First cause" );
        RuntimeException secondCause = new RuntimeException( "Second cause" );

        // and strategies fail with given causes
        when( firstStrategy.doBackup( any() ) )
                .thenReturn( new PotentiallyErroneousState<>( BackupStrategyOutcome.INCORRECT_STRATEGY, firstCause ) );
        when( secondStrategy.doBackup( any() ) )
                .thenReturn( new PotentiallyErroneousState<>( BackupStrategyOutcome.INCORRECT_STRATEGY, secondCause ) );

        // then the command failed exception contains the specified causes
        expectedException.expect( exceptionContainsSuppressedThrowable( firstCause ) );
        expectedException.expect( exceptionContainsSuppressedThrowable( secondCause ) );
        expectedException.expect( CommandFailed.class );
        expectedException.expectMessage( "Failed to run a backup using the available strategies." );

        // when
        subject.performBackup( onlineBackupContext );
    }

    @Test
    public void errorsDuringConsistencyCheckAreWrappedAsCommandFailed() throws CommandFailed, IOException, ConsistencyCheckIncompleteException
    {
        // given
        anyStrategyPasses();
        when( requiredArguments.isDoConsistencyCheck() ).thenReturn( true );
        when( consistencyCheckService.runFullConsistencyCheck( any(), any(),
                eq(progressMonitorFactory), any( LogProvider.class ), any( FileSystemAbstraction.class),
                eq(false), any(File.class), any( ConsistencyFlags.class ) ) )
                .thenThrow( new IOException( "Predictable message" ) );

        // then
        expectedException.expect( CommandFailed.class );
        expectedException.expectMessage( containsString( "Failed to do consistency check on backup: Predictable message" ) );

        // when
        subject.performBackup( onlineBackupContext );
    }

    @Test
    public void commandFailedWhenConsistencyCheckFails() throws IOException, ConsistencyCheckIncompleteException, CommandFailed
    {
        // given
        anyStrategyPasses();
        when( requiredArguments.isDoConsistencyCheck() ).thenReturn( true );
        when( consistencyCheckResult.isSuccessful() ).thenReturn( false );
        when( consistencyCheckService.runFullConsistencyCheck( any(), any(),
                eq(progressMonitorFactory), any( LogProvider.class ), any( FileSystemAbstraction.class),
                eq(false), any(File.class), any( ConsistencyFlags.class ) ) )
                .thenReturn( consistencyCheckResult );

        // then
        expectedException.expect( CommandFailed.class );
        expectedException.expectMessage( "Inconsistencies found" );

        // when
        subject.performBackup( onlineBackupContext );
    }

    @Test
    public void havingNoStrategiesCausesAllSolutionsFailedException() throws CommandFailed
    {
        // given there are no strategies in the solution
        subject = new BackupFlow( consistencyCheckService, outsideWorld, logProvider, progressMonitorFactory, Collections.emptyList() );

        // then we want a predictable exception (instead of NullPointer)
        expectedException.expect( CommandFailed.class );
        expectedException.expectMessage( "Failed to run a backup using the available strategies." );

        // when
        subject.performBackup( onlineBackupContext );
    }

    /**
     * Fixture for other tests
     */
    private void anyStrategyPasses()
    {
        when( firstStrategy.doBackup( any() ) ).thenReturn( new PotentiallyErroneousState<>( BackupStrategyOutcome.SUCCESS, null ) );
        when( secondStrategy.doBackup( any() )).thenReturn( new PotentiallyErroneousState<>( BackupStrategyOutcome.SUCCESS, null ) );
    }
}
