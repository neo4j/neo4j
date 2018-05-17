/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.backup.impl;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.io.IOException;
import java.nio.file.Path;

import org.neo4j.causalclustering.catchup.CatchupResult;
import org.neo4j.causalclustering.catchup.storecopy.StoreCopyFailedException;
import org.neo4j.causalclustering.catchup.storecopy.StoreFiles;
import org.neo4j.causalclustering.catchup.storecopy.StoreIdDownloadFailedException;
import org.neo4j.causalclustering.identity.StoreId;
import org.neo4j.helpers.AdvertisedSocketAddress;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.util.OptionalHostnamePort;
import org.neo4j.logging.NullLogProvider;

import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class CausalClusteringBackupStrategyTest
{
    @Rule
    public ExpectedException expected = ExpectedException.none();

    BackupDelegator backupDelegator = mock( BackupDelegator.class );
    AddressResolver addressResolver = mock( AddressResolver.class );
    AdvertisedSocketAddress resolvedFromAddress = new AdvertisedSocketAddress( "resolved-host", 1358 );

    CausalClusteringBackupStrategy subject;

    Path desiredBackupLocation = mock( Path.class );
    Config config = mock( Config.class );
    OptionalHostnamePort userProvidedAddress = new OptionalHostnamePort( (String) null, null, null );
    StoreFiles storeFiles = mock( StoreFiles.class );
    StoreId expectedStoreId = new StoreId( 11, 22, 33, 44 );

    @Before
    public void setup() throws IOException, StoreIdDownloadFailedException
    {
        when( addressResolver.resolveCorrectCCAddress( any(), any() ) ).thenReturn( resolvedFromAddress );
        when( storeFiles.readStoreId( any() ) ).thenReturn( expectedStoreId );
        when( backupDelegator.fetchStoreId( any() ) ).thenReturn( expectedStoreId );
        subject = new CausalClusteringBackupStrategy( backupDelegator, addressResolver, NullLogProvider.getInstance(), storeFiles );
    }

    @Test
    public void incrementalBackupsUseCorrectResolvedAddress() throws StoreCopyFailedException
    {
        // given
        AdvertisedSocketAddress expectedAddress = new AdvertisedSocketAddress( "expected-host", 1298 );
        when( addressResolver.resolveCorrectCCAddress( any(), any() ) ).thenReturn( expectedAddress );

        // when
        subject.performIncrementalBackup( desiredBackupLocation, config, userProvidedAddress );

        // then
        verify( backupDelegator ).tryCatchingUp( eq( expectedAddress ), any(), any() );
    }

    @Test
    public void fullBackupUsesCorrectResolvedAddress() throws StoreIdDownloadFailedException
    {
        // given
        AdvertisedSocketAddress expectedAddress = new AdvertisedSocketAddress( "expected-host", 1578 );
        when( addressResolver.resolveCorrectCCAddress( any(), any() ) ).thenReturn( expectedAddress );

        // when
        subject.performFullBackup( desiredBackupLocation, config, userProvidedAddress );

        // then
        verify( backupDelegator ).fetchStoreId( expectedAddress );
    }

    @Test
    public void incrementalRunsCatchupWithTargetsStoreId() throws StoreIdDownloadFailedException, StoreCopyFailedException
    {

        // when
        subject.performIncrementalBackup( desiredBackupLocation, config, userProvidedAddress );

        // then
        verify( backupDelegator ).fetchStoreId( resolvedFromAddress );
        verify( backupDelegator ).tryCatchingUp( eq( resolvedFromAddress ), eq( expectedStoreId ), any() );
    }

    @Test
    public void fullRunsRetrieveStoreWithTargetsStoreId() throws StoreIdDownloadFailedException, StoreCopyFailedException, IOException
    {
        // given
        when( storeFiles.readStoreId( any() ) ).thenThrow( IOException.class );

        // when
        subject.performFullBackup( desiredBackupLocation, config, userProvidedAddress );

        // then
        verify( backupDelegator ).fetchStoreId( resolvedFromAddress );
        verify( backupDelegator ).copy( resolvedFromAddress, expectedStoreId, desiredBackupLocation );
    }

    @Test
    public void failingToRetrieveStoreIdCausesFailWithStatus_incrementalBackup() throws StoreIdDownloadFailedException
    {
        // given
        StoreIdDownloadFailedException storeIdDownloadFailedException = new StoreIdDownloadFailedException( "Expected description" );
        when( backupDelegator.fetchStoreId( any() ) ).thenThrow( storeIdDownloadFailedException );

        // when
        Fallible<BackupStageOutcome>
                state = subject.performIncrementalBackup( desiredBackupLocation, config, userProvidedAddress );

        // then
        assertEquals( BackupStageOutcome.WRONG_PROTOCOL, state.getState() );
        assertEquals( storeIdDownloadFailedException, state.getCause().get() );
    }

    @Test
    public void failingToCopyStoresCausesFailWithStatus_incrementalBackup() throws StoreIdDownloadFailedException, StoreCopyFailedException
    {
        // given
        when( backupDelegator.tryCatchingUp( any(), eq( expectedStoreId ), any() ) ).thenThrow( StoreCopyFailedException.class );

        // when
        Fallible state = subject.performIncrementalBackup( desiredBackupLocation, config, userProvidedAddress );

        // then
        assertEquals( BackupStageOutcome.FAILURE, state.getState() );
        assertEquals( StoreCopyFailedException.class, state.getCause().get().getClass() );
    }

    @Test
    public void failingToRetrieveStoreIdCausesFailWithStatus_fullBackup() throws StoreIdDownloadFailedException
    {
        // given
        StoreIdDownloadFailedException storeIdDownloadFailedException = new StoreIdDownloadFailedException( "Expected description" );
        when( backupDelegator.fetchStoreId( any() ) ).thenThrow( storeIdDownloadFailedException );

        // when
        Fallible state = subject.performFullBackup( desiredBackupLocation, config, userProvidedAddress );

        // then
        assertEquals( BackupStageOutcome.WRONG_PROTOCOL, state.getState() );
        assertEquals( storeIdDownloadFailedException, state.getCause().get() );
    }

    @Test
    public void failingToCopyStoresCausesFailWithStatus_fullBackup() throws StoreCopyFailedException, IOException
    {
        // given
        doThrow( StoreCopyFailedException.class ).when( backupDelegator ).copy( any(), any(), any() );

        // and
        when( storeFiles.readStoreId( any() ) ).thenThrow( IOException.class );

        // when
        Fallible state = subject.performFullBackup( desiredBackupLocation, config, userProvidedAddress );

        // then
        assertEquals( BackupStageOutcome.FAILURE, state.getState() );
        System.out.println( state.getCause() );
        assertEquals( StoreCopyFailedException.class, state.getCause().get().getClass() );
    }

    @Test
    public void incrementalBackupsEndingInUnacceptedCatchupStateCauseFailures() throws StoreCopyFailedException
    {
        // given
        when( backupDelegator.tryCatchingUp( any(), any(), any() ) ).thenReturn( CatchupResult.E_STORE_UNAVAILABLE );

        // when
        Fallible<BackupStageOutcome>
                state = subject.performIncrementalBackup( desiredBackupLocation, config, userProvidedAddress );

        // then
        assertEquals( BackupStageOutcome.FAILURE, state.getState() );
        assertEquals( StoreCopyFailedException.class, state.getCause().get().getClass() );
        assertEquals( "End state of catchup was not a successful end of stream", state.getCause().get().getMessage() );
    }

    @Test
    public void lifecycleDelegatesToNecessaryServices() throws Throwable
    {
        // when
        subject.start();

        // then
        verify( backupDelegator ).start();
        verify( backupDelegator, never() ).stop();

        // when
        subject.stop();

        // then
        verify( backupDelegator ).start(); // still total 1 calls
        verify( backupDelegator ).stop();
    }

    @Test
    public void exceptionWhenStoreMismatchNoExistingBackup() throws IOException
    {
        // given
        when( storeFiles.readStoreId( any() ) ).thenThrow( IOException.class );

        // when
        Fallible<BackupStageOutcome> state = subject.performIncrementalBackup( desiredBackupLocation, config, userProvidedAddress );

        // then
        assertEquals( StoreIdDownloadFailedException.class, state.getCause().get().getClass() );
        assertEquals( BackupStageOutcome.FAILURE, state.getState() );
    }

    @Test
    public void exceptionWhenStoreMismatch() throws IOException
    {
        // given
        when( storeFiles.readStoreId( any() ) ).thenReturn( new StoreId( 5, 4, 3, 2 ) );

        // when
        Fallible<BackupStageOutcome> state = subject.performIncrementalBackup( desiredBackupLocation, config, userProvidedAddress );

        // then
        assertEquals( StoreIdDownloadFailedException.class, state.getCause().get().getClass() );
        assertEquals( BackupStageOutcome.FAILURE, state.getState() );
    }

    @Test
    public void fullBackupFailsWhenTargetHasStoreId() throws IOException
    {
        // given
        when( storeFiles.readStoreId( any() ) ).thenReturn( expectedStoreId );

        // when
        Fallible<BackupStageOutcome> state = subject.performFullBackup( desiredBackupLocation, config, userProvidedAddress );

        // then
        assertEquals( StoreIdDownloadFailedException.class, state.getCause().get().getClass() );
        assertEquals( BackupStageOutcome.FAILURE, state.getState() );
    }
}
