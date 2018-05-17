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
package org.neo4j.causalclustering.stresstests;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.ThreadLocalRandom;

import org.neo4j.backup.impl.OnlineBackupCommandBuilder;
import org.neo4j.causalclustering.core.CausalClusteringSettings;
import org.neo4j.causalclustering.discovery.Cluster;
import org.neo4j.causalclustering.discovery.ClusterMember;
import org.neo4j.causalclustering.discovery.CoreClusterMember;
import org.neo4j.commandline.admin.CommandFailed;
import org.neo4j.commandline.admin.IncorrectUsage;
import org.neo4j.helpers.AdvertisedSocketAddress;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.logging.Log;

import static org.neo4j.backup.impl.SelectedBackupProtocol.CATCHUP;
import static org.neo4j.causalclustering.BackupUtil.restoreFromBackup;
import static org.neo4j.io.NullOutputStream.NULL_OUTPUT_STREAM;

class ReplaceRandomMember extends RepeatOnRandomMember
{
    private final Cluster cluster;
    private final File baseBackupDir;
    private final FileSystemAbstraction fs;
    private final Log log;

    private int backupNumber;

    ReplaceRandomMember( Control control, Resources resources )
    {
        super( control, resources );
        this.cluster = resources.cluster();
        this.baseBackupDir = resources.backupDir();
        this.fs = resources.fileSystem();
        this.log = resources.logProvider().getLog( getClass() );
    }

    @Override
    protected void doWorkOnMember( ClusterMember oldMember ) throws CommandFailed, IncorrectUsage, IOException
    {
        File backupDir = null;

        String backupName = null;
        boolean replaceFromBackup = ThreadLocalRandom.current().nextBoolean();

        if ( replaceFromBackup )
        {
            backupName = "backup-" + backupNumber++;

            AdvertisedSocketAddress address = oldMember.config().get( CausalClusteringSettings.transaction_advertised_address );
            backupDir = new File( baseBackupDir, backupName );

            new OnlineBackupCommandBuilder()
                    .withOutput( NULL_OUTPUT_STREAM )
                    .withSelectedBackupStrategy( CATCHUP )
                    .withConsistencyCheck( false )
                    .withHost( address.getHostname() )
                    .withPort( address.getPort() )
                    .backup( baseBackupDir, backupName );

            log.info( "Created backup: " + backupName + " from: " + oldMember );
        }

        log.info( "Stopping: " + oldMember );
        oldMember.shutdown();

        ClusterMember newMember = (oldMember instanceof CoreClusterMember) ?
                cluster.newCoreMember() :
                cluster.newReadReplica();

        if ( backupDir != null )
        {
            log.info( "Restoring backup: " + backupName + " to: " + newMember );
            restoreFromBackup( backupDir, fs, newMember );
        }

        log.info( "Starting: " + newMember );
        newMember.start();
    }
}
