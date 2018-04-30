/*
 * Copyright (c) 2002-2018 "Neo4j,"
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
package org.neo4j.causalclustering.stresstests;

import org.hamcrest.Matchers;

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

import static java.lang.String.format;
import static org.junit.Assert.assertThat;
import static org.neo4j.backup.impl.SelectedBackupProtocol.CATCHUP;
import static org.neo4j.causalclustering.BackupUtil.restoreFromBackup;
import static org.neo4j.io.NullOutputStream.NULL_OUTPUT_STREAM;

class ReplaceRandomMember extends RepeatOnRandomMember
{
    /* Basic pass criteria for the stress test. We must have replaced at least two members. */
    private static final int MIN_SUCCESSFUL_REPLACEMENTS = 2;

    /* Backups retry a few times with a pause in between. */
    private static final long MAX_BACKUP_FAILURES = 5;
    private static final long RETRY_TIMEOUT_MILLIS = 5000;

    private final Cluster<?> cluster;
    private final File baseBackupDir;
    private final FileSystemAbstraction fs;
    private final Log log;

    private int backupNumber;
    private int successfulReplacements;

    ReplaceRandomMember( Control control, Resources resources )
    {
        super( control, resources );
        this.cluster = resources.cluster();
        this.baseBackupDir = resources.backupDir();
        this.fs = resources.fileSystem();
        this.log = resources.logProvider().getLog( getClass() );
    }

    @Override
    protected void doWorkOnMember( ClusterMember oldMember ) throws CommandFailed, IncorrectUsage, IOException,
            InterruptedException
    {
        File backupDir = null;

        String backupName = null;
        boolean replaceFromBackup = ThreadLocalRandom.current().nextBoolean();

        if ( replaceFromBackup )
        {
            backupName = "backup-" + backupNumber++;

            AdvertisedSocketAddress address = oldMember.config().get(
                    CausalClusteringSettings.transaction_advertised_address );
            backupDir = new File( baseBackupDir, backupName );

            log.info( "Creating backup: " + backupName + " from: " + oldMember );
            createBackupWithRetries( backupName, address );
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

        successfulReplacements++;
    }

    private void createBackupWithRetries( String backupName, AdvertisedSocketAddress address ) throws IncorrectUsage,
            InterruptedException, CommandFailed
    {
        int failureCount = 0;

        boolean done = false;
        while ( !done )
        {
            try
            {
                new OnlineBackupCommandBuilder().withOutput( NULL_OUTPUT_STREAM )
                        .withSelectedBackupStrategy( CATCHUP )
                        .withConsistencyCheck( false )
                        .withHost( address.getHostname() )
                        .withPort( address.getPort() )
                        .backup( baseBackupDir, backupName );

                done = true;
            }
            catch ( CommandFailed e )
            {
                log.warn( format( "Failed backup: %s from: %s.", backupName, address ), e );
                failureCount++;

                if ( failureCount >= MAX_BACKUP_FAILURES )
                {
                    throw new RuntimeException( format( "Backup failed %s times in a row.", failureCount ) );
                }

                log.info( "Retrying backup in %s ms.", RETRY_TIMEOUT_MILLIS );
                Thread.sleep( RETRY_TIMEOUT_MILLIS );
            }
        }
    }

    @Override
    public void validate()
    {
        assertThat( successfulReplacements, Matchers.greaterThanOrEqualTo( MIN_SUCCESSFUL_REPLACEMENTS ) );
    }
}
