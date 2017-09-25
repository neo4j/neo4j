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

import java.io.File;

import org.neo4j.com.ComException;
import org.neo4j.helpers.HostnamePort;
import org.neo4j.helpers.OptionalHostnamePort;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;

public class HaBackupStrategy extends LifecycleAdapter implements BackupStrategy
{
    private final BackupProtocolService backupProtocolService;
    private final AddressResolutionHelper addressResolutionHelper;
    private final long timeout;

    HaBackupStrategy( BackupProtocolService backupProtocolService, AddressResolutionHelper addressResolutionHelper, long timeout )
    {
        this.backupProtocolService = backupProtocolService;
        this.addressResolutionHelper = addressResolutionHelper;
        this.timeout = timeout;
    }

    @Override
    public PotentiallyErroneousState<BackupStageOutcome> performIncrementalBackup( File backupDestination, Config config, OptionalHostnamePort fromAddress )
    {
        HostnamePort resolvedAddress = addressResolutionHelper.resolveCorrectHAAddress( config, fromAddress );
        try
        {
            backupProtocolService.doIncrementalBackup( resolvedAddress.getHost(), resolvedAddress.getPort(), backupDestination, ConsistencyCheck.NONE, timeout,
                    config );
            return new PotentiallyErroneousState<>( BackupStageOutcome.SUCCESS, null );
        }
        catch ( IncrementalBackupNotPossibleException e )
        {
            return new PotentiallyErroneousState<>( BackupStageOutcome.FAILURE, e );
            // This means the existing backup was so old, that an incremental backup was not possible
        }
        catch ( RuntimeException e )
        {
            return new PotentiallyErroneousState<>( BackupStageOutcome.WRONG_PROTOCOL, e );
        }
    }

    @Override
    public PotentiallyErroneousState<BackupStageOutcome> performFullBackup( File desiredBackupLocation, Config config,
            OptionalHostnamePort userProvidedAddress )
    {
        HostnamePort fromAddress = addressResolutionHelper.resolveCorrectHAAddress( config, userProvidedAddress );
        ConsistencyCheck consistencyCheck = ConsistencyCheck.NONE;
        boolean forensics = false;
        try
        {
            backupProtocolService.doFullBackup( fromAddress.getHost(), fromAddress.getPort(), desiredBackupLocation, consistencyCheck, config, timeout,
                    forensics );
            return new PotentiallyErroneousState<>( BackupStageOutcome.SUCCESS, null );
        }
        catch ( ComException e )
        {
            return new PotentiallyErroneousState<>( BackupStageOutcome.WRONG_PROTOCOL, e );
        }
    }
}
