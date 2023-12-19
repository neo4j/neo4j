/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * Neo4j Sweden Software License, as found in the associated LICENSE.txt
 * file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * Neo4j Sweden Software License for more details.
 */
package org.neo4j.backup.impl;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.util.Optional;

import org.neo4j.causalclustering.catchup.CatchupResult;
import org.neo4j.causalclustering.catchup.storecopy.StoreCopyFailedException;
import org.neo4j.causalclustering.catchup.storecopy.StoreFiles;
import org.neo4j.causalclustering.catchup.storecopy.StoreIdDownloadFailedException;
import org.neo4j.causalclustering.identity.StoreId;
import org.neo4j.helpers.AdvertisedSocketAddress;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.util.OptionalHostnamePort;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;

import static java.lang.String.format;

class CausalClusteringBackupStrategy extends LifecycleAdapter implements BackupStrategy
{
    private final BackupDelegator backupDelegator;
    private final AddressResolver addressResolver;
    private final Log log;
    private final StoreFiles storeFiles;

    CausalClusteringBackupStrategy( BackupDelegator backupDelegator, AddressResolver addressResolver, LogProvider logProvider, StoreFiles storeFiles )
    {
        this.backupDelegator = backupDelegator;
        this.addressResolver = addressResolver;
        this.log = logProvider.getLog( CausalClusteringBackupStrategy.class );
        this.storeFiles = storeFiles;
    }

    @Override
    public Fallible<BackupStageOutcome> performFullBackup( Path desiredBackupLocation, Config config,
                                                           OptionalHostnamePort userProvidedAddress )
    {
        AdvertisedSocketAddress fromAddress = addressResolver.resolveCorrectCCAddress( config, userProvidedAddress );
        log.info( "Resolved address for catchup protocol is " + fromAddress );
        StoreId storeId;
        try
        {
            storeId = backupDelegator.fetchStoreId( fromAddress );
            log.info( "Remote store id is " + storeId );
        }
        catch ( StoreIdDownloadFailedException e )
        {
            return new Fallible<>( BackupStageOutcome.WRONG_PROTOCOL, e );
        }

        Optional<StoreId> expectedStoreId = readLocalStoreId( desiredBackupLocation.toFile() );
        if ( expectedStoreId.isPresent() )
        {
            return new Fallible<>( BackupStageOutcome.FAILURE, new StoreIdDownloadFailedException(
                    format( "Cannot perform a full backup onto preexisting backup. Remote store id was %s but local is %s", storeId, expectedStoreId ) ) );
        }

        try
        {
            backupDelegator.copy( fromAddress, storeId, desiredBackupLocation );
            return new Fallible<>( BackupStageOutcome.SUCCESS, null );
        }
        catch ( StoreCopyFailedException e )
        {
            return new Fallible<>( BackupStageOutcome.FAILURE, e );
        }
    }

    @Override
    public Fallible<BackupStageOutcome> performIncrementalBackup( Path desiredBackupLocation, Config config,
                                                                  OptionalHostnamePort userProvidedAddress )
    {
        AdvertisedSocketAddress fromAddress = addressResolver.resolveCorrectCCAddress( config, userProvidedAddress );
        log.info( "Resolved address for catchup protocol is " + fromAddress );
        StoreId storeId;
        try
        {
            storeId = backupDelegator.fetchStoreId( fromAddress );
            log.info( "Remote store id is " + storeId );
        }
        catch ( StoreIdDownloadFailedException e )
        {
            return new Fallible<>( BackupStageOutcome.WRONG_PROTOCOL, e );
        }
        Optional<StoreId> expectedStoreId = readLocalStoreId( desiredBackupLocation.toFile() );
        if ( !expectedStoreId.isPresent() || !expectedStoreId.get().equals( storeId ) )
        {
            return new Fallible<>( BackupStageOutcome.FAILURE,
                    new StoreIdDownloadFailedException( format( "Remote store id was %s but local is %s", storeId, expectedStoreId ) ) );
        }
        return catchup( fromAddress, storeId, desiredBackupLocation );
    }

    @Override
    public void start() throws Throwable
    {
        super.start();
        backupDelegator.start();
    }

    @Override
    public void stop() throws Throwable
    {
        backupDelegator.stop();
        super.stop();
    }

    private Optional<StoreId> readLocalStoreId( File backupLocation )
    {
        try
        {
            return Optional.of( storeFiles.readStoreId( backupLocation ) );
        }
        catch ( IOException e )
        {
            return Optional.empty();
        }
    }

    private Fallible<BackupStageOutcome> catchup( AdvertisedSocketAddress fromAddress, StoreId storeId, Path backupTarget )
    {
        CatchupResult catchupResult;
        try
        {
            catchupResult = backupDelegator.tryCatchingUp( fromAddress, storeId, backupTarget );
        }
        catch ( StoreCopyFailedException e )
        {
            return new Fallible<>( BackupStageOutcome.FAILURE, e );
        }
        if ( catchupResult == CatchupResult.SUCCESS_END_OF_STREAM )
        {
            return new Fallible<>( BackupStageOutcome.SUCCESS, null );
        }
        return new Fallible<>( BackupStageOutcome.FAILURE,
                new StoreCopyFailedException( "End state of catchup was not a successful end of stream" ) );
    }
}
