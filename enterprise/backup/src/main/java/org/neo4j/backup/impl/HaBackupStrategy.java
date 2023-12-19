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

import java.nio.file.Path;

import org.neo4j.com.ComException;
import org.neo4j.helpers.HostnamePort;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.store.MismatchingStoreIdException;
import org.neo4j.kernel.impl.util.OptionalHostnamePort;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;

class HaBackupStrategy extends LifecycleAdapter implements BackupStrategy
{
    private final BackupProtocolService backupProtocolService;
    private final AddressResolver addressResolver;
    private final long timeout;
    private final Log log;

    HaBackupStrategy( BackupProtocolService backupProtocolService, AddressResolver addressResolver, LogProvider logProvider, long timeout )
    {
        this.backupProtocolService = backupProtocolService;
        this.addressResolver = addressResolver;
        this.timeout = timeout;
        this.log = logProvider.getLog( HaBackupStrategy.class );
    }

    @Override
    public Fallible<BackupStageOutcome> performIncrementalBackup( Path backupDestination, Config config, OptionalHostnamePort fromAddress )
    {
        HostnamePort resolvedAddress = addressResolver.resolveCorrectHAAddress( config, fromAddress );
        log.info( "Resolved address for backup protocol is " + resolvedAddress );
        try
        {
            String host = resolvedAddress.getHost();
            int port = resolvedAddress.getPort();
            backupProtocolService.doIncrementalBackup(
                    host, port, backupDestination, ConsistencyCheck.NONE, timeout, config );
            return new Fallible<>( BackupStageOutcome.SUCCESS, null );
        }
        catch ( MismatchingStoreIdException e )
        {
            return new Fallible<>( BackupStageOutcome.UNRECOVERABLE_FAILURE, e );
        }
        catch ( RuntimeException e )
        {
            return new Fallible<>( BackupStageOutcome.FAILURE, e );
        }
    }

    @Override
    public Fallible<BackupStageOutcome> performFullBackup( Path desiredBackupLocation, Config config,
                                                           OptionalHostnamePort userProvidedAddress )
    {
        HostnamePort fromAddress = addressResolver.resolveCorrectHAAddress( config, userProvidedAddress );
        log.info( "Resolved address for backup protocol is " + fromAddress );
        ConsistencyCheck consistencyCheck = ConsistencyCheck.NONE;
        boolean forensics = false;
        try
        {
            String host = fromAddress.getHost();
            int port = fromAddress.getPort();
            backupProtocolService.doFullBackup(
                    host, port, desiredBackupLocation, consistencyCheck, config, timeout, forensics );
            return new Fallible<>( BackupStageOutcome.SUCCESS, null );
        }
        catch ( ComException e )
        {
            return new Fallible<>( BackupStageOutcome.WRONG_PROTOCOL, e );
        }
    }
}
