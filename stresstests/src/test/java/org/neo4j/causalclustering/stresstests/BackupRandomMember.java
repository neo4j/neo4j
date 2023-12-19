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
package org.neo4j.causalclustering.stresstests;

import java.io.File;

import org.neo4j.backup.impl.OnlineBackupCommandBuilder;
import org.neo4j.causalclustering.core.CausalClusteringSettings;
import org.neo4j.causalclustering.discovery.ClusterMember;
import org.neo4j.commandline.admin.CommandFailed;
import org.neo4j.commandline.admin.IncorrectUsage;
import org.neo4j.helpers.AdvertisedSocketAddress;
import org.neo4j.logging.Log;

import static org.neo4j.backup.impl.SelectedBackupProtocol.CATCHUP;
import static org.neo4j.io.NullOutputStream.NULL_OUTPUT_STREAM;

class BackupRandomMember extends RepeatOnRandomMember
{
    private final File baseBackupDir;
    private final Log log;
    private long backupNumber;
    private long successfulBackups;

    BackupRandomMember( Control control, Resources resources )
    {
        super( control, resources );
        this.baseBackupDir = resources.backupDir();
        this.log = resources.logProvider().getLog( getClass() );
    }

    @Override
    protected void doWorkOnMember( ClusterMember member ) throws IncorrectUsage
    {
        try
        {
            AdvertisedSocketAddress address = member.config().get( CausalClusteringSettings.transaction_advertised_address );

            String backupName = "backup-" + backupNumber++;

            new OnlineBackupCommandBuilder()
                    .withOutput( NULL_OUTPUT_STREAM )
                    .withSelectedBackupStrategy( CATCHUP )
                    .withConsistencyCheck( true )
                    .withHost( address.getHostname() )
                    .withPort( address.getPort() )
                    .backup( baseBackupDir, backupName );

            log.info( String.format( "Created backup %s from %s", backupName, member ) );
            successfulBackups++;
        }
        catch ( CommandFailed e )
        {
            log.info( "Backup command failed" );
        }
    }

    @Override
    public void validate()
    {
        if ( successfulBackups == 0 )
        {
            throw new IllegalStateException( "Failed to perform any backups" );
        }

        log.info( String.format( "Performed %d/%d successful backups.", successfulBackups, backupNumber ) );
    }
}
