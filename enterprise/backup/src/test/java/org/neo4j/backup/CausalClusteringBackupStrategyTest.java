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

import org.neo4j.causalclustering.catchup.CatchupResult;
import org.neo4j.causalclustering.catchup.storecopy.StoreCopyFailedException;
import org.neo4j.causalclustering.catchup.storecopy.StoreIdDownloadFailedException;
import org.neo4j.causalclustering.identity.StoreId;
import org.neo4j.helpers.AdvertisedSocketAddress;
import org.neo4j.helpers.OptionalHostnamePort;
import org.neo4j.kernel.configuration.Config;

import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
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
    AddressResolutionHelper addressResolutionHelper = mock( AddressResolutionHelper.class );
    AdvertisedSocketAddress resolvedFromAddress = new AdvertisedSocketAddress( "resolved-host", 1358 );

    CausalClusteringBackupStrategy subject;

    File desiredBackupLocation = mock( File.class );
    Config config = mock( Config.class );
    OptionalHostnamePort userProvidedAddress = new OptionalHostnamePort( (String) null, null, null );

    @Before
    public void setup()
    {
        when( addressResolutionHelper.resolveCorrectCCAddress( any(), any() ) ).thenReturn( resolvedFromAddress );
        subject = new CausalClusteringBackupStrategy( backupDelegator, addressResolutionHelper );
    }

    @Test
    public void incrementalBackupsUseCorrectResolvedAddress() throws StoreCopyFailedException
    {
        // given
        AdvertisedSocketAddress expectedAddress = new AdvertisedSocketAddress( "expected-host", 1298 );
        when( addressResolutionHelper.resolveCorrectCCAddress( any(), any() ) ).thenReturn( expectedAddress );

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
        when( addressResolutionHelper.resolveCorrectCCAddress( any(), any() ) ).thenReturn( expectedAddress );

        // when
        subject.performFullBackup( desiredBackupLocation, config, userProvidedAddress );

        // then
        verify( backupDelegator ).fetchStoreId( expectedAddress );
    }

    @Test
    public void incrementalRunsCatchupWithTargetsStoreId() throws StoreIdDownloadFailedException, StoreCopyFailedException
    {
        // given
        StoreId storeId = anyStoreId();

        when( backupDelegator.fetchStoreId( resolvedFromAddress ) ).thenReturn( storeId );

        // when
        subject.performIncrementalBackup( desiredBackupLocation, config, userProvidedAddress );

        // then
        verify( backupDelegator ).fetchStoreId( resolvedFromAddress );
        verify( backupDelegator ).tryCatchingUp( eq( resolvedFromAddress ), eq( storeId ), any() );
    }

    @Test
    public void fullRunsRetrieveStoreWithTargetsStoreId() throws StoreIdDownloadFailedException, StoreCopyFailedException
    {
        // given
        StoreId storeId = anyStoreId();
        when( backupDelegator.fetchStoreId( resolvedFromAddress ) ).thenReturn( storeId );

        // when
        subject.performFullBackup( desiredBackupLocation, config, userProvidedAddress );

        // then
        verify( backupDelegator ).fetchStoreId( resolvedFromAddress );
        verify( backupDelegator ).copy( resolvedFromAddress, storeId, desiredBackupLocation );
    }

    @Test
    public void failingToRetrieveStoreIdCausesFailWithStatus_incrementalBackup() throws StoreIdDownloadFailedException
    {
        // given
        AdvertisedSocketAddress fromAddress = anyAddress();
        StoreIdDownloadFailedException storeIdDownloadFailedException = new StoreIdDownloadFailedException( "Expected description" );
        when( backupDelegator.fetchStoreId( any() ) ).thenThrow( storeIdDownloadFailedException );

        // when
        PotentiallyErroneousState<BackupStageOutcome> state = subject.performIncrementalBackup( desiredBackupLocation, config, userProvidedAddress );

        // then
        assertEquals( BackupStageOutcome.WRONG_PROTOCOL, state.getState() );
        assertEquals( storeIdDownloadFailedException, state.getCause().get() );
    }

    @Test
    public void failingToCopyStoresCausesFailWithStatus_incrementalBackup() throws StoreIdDownloadFailedException, StoreCopyFailedException
    {
        // given
        StoreId storeId = anyStoreId();
        when( backupDelegator.fetchStoreId( any() ) ).thenReturn( storeId );
        when( backupDelegator.tryCatchingUp( any(), eq( storeId ), any() ) ).thenThrow( StoreCopyFailedException.class );

        // when
        PotentiallyErroneousState state = subject.performIncrementalBackup( desiredBackupLocation, config, userProvidedAddress );

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
        PotentiallyErroneousState state = subject.performFullBackup( desiredBackupLocation, config, userProvidedAddress );

        // then
        assertEquals( BackupStageOutcome.WRONG_PROTOCOL, state.getState() );
        assertEquals( storeIdDownloadFailedException, state.getCause().get() );
    }

    @Test
    public void failingToCopyStoresCausesFailWithStatus_fullBackup() throws StoreCopyFailedException
    {
        // given
        doThrow( StoreCopyFailedException.class ).when( backupDelegator ).copy( any(), any(), any() );

        // when
        PotentiallyErroneousState state = subject.performFullBackup( desiredBackupLocation, config, userProvidedAddress );

        // then
        assertEquals( BackupStageOutcome.FAILURE, state.getState() );
        assertEquals( StoreCopyFailedException.class, state.getCause().get().getClass() );
    }

    @Test
    public void incrementalBackupsEndingInUnacceptedCatchupStateCauseFailures() throws StoreCopyFailedException
    {
        // given
        when( backupDelegator.tryCatchingUp( any(), any(), any() ) ).thenReturn( CatchupResult.E_STORE_UNAVAILABLE );

        // when
        PotentiallyErroneousState<BackupStageOutcome> state = subject.performIncrementalBackup( desiredBackupLocation, config, userProvidedAddress );

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

    private StoreId anyStoreId()
    {
        return new StoreId( 1, 2, 3, 4 );
    }

    private AdvertisedSocketAddress anyAddress()
    {
        return new AdvertisedSocketAddress( "hostname", 1234 );
    }
}
