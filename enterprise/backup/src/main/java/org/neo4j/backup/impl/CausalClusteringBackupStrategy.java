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

import java.nio.file.Path;

import org.neo4j.causalclustering.catchup.CatchupResult;
import org.neo4j.causalclustering.catchup.storecopy.StoreCopyFailedException;
import org.neo4j.causalclustering.catchup.storecopy.StoreIdDownloadFailedException;
import org.neo4j.causalclustering.identity.StoreId;
import org.neo4j.helpers.AdvertisedSocketAddress;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.util.OptionalHostnamePort;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;

class CausalClusteringBackupStrategy extends LifecycleAdapter implements BackupStrategy
{
    private final BackupDelegator backupDelegator;
    private final AddressResolver addressResolver;

    CausalClusteringBackupStrategy( BackupDelegator backupDelegator, AddressResolver addressResolver )
    {
        this.backupDelegator = backupDelegator;
        this.addressResolver = addressResolver;
    }

    @Override
    public Fallible<BackupStageOutcome> performFullBackup( Path desiredBackupLocation, Config config,
                                                           OptionalHostnamePort userProvidedAddress )
    {
        AdvertisedSocketAddress fromAddress = addressResolver.resolveCorrectCCAddress( config, userProvidedAddress );
        StoreId storeId;
        try
        {
            storeId = backupDelegator.fetchStoreId( fromAddress );
        }
        catch ( StoreIdDownloadFailedException e )
        {
            return new Fallible<>( BackupStageOutcome.WRONG_PROTOCOL, e );
        }

        try
        {
            backupDelegator.copy( fromAddress,storeId, desiredBackupLocation );
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
        StoreId storeId;
        try
        {
            storeId = backupDelegator.fetchStoreId( fromAddress );
        }
        catch ( StoreIdDownloadFailedException e )
        {
            return new Fallible<>( BackupStageOutcome.WRONG_PROTOCOL, e );
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
