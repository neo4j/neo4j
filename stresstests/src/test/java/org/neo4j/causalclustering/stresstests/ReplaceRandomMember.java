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
import java.util.Optional;
import java.util.concurrent.ThreadLocalRandom;

import org.neo4j.causalclustering.discovery.Cluster;
import org.neo4j.causalclustering.discovery.ClusterMember;
import org.neo4j.causalclustering.discovery.CoreClusterMember;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.logging.Log;

import static java.lang.String.format;
import static org.junit.Assert.assertThat;
import static org.neo4j.causalclustering.BackupUtil.restoreFromBackup;

class ReplaceRandomMember extends RepeatOnRandomMember
{
    /* Basic pass criteria for the stress test. We must have replaced at least two members. */
    private static final int MIN_SUCCESSFUL_REPLACEMENTS = 2;

    /* Backups retry a few times with a pause in between. */
    private static final long MAX_BACKUP_FAILURES = 20;
    private static final long RETRY_TIMEOUT_MILLIS = 5000;

    private final Cluster<?> cluster;
    private final FileSystemAbstraction fs;
    private final Log log;
    private final BackupHelper backupHelper;

    private int successfulReplacements;

    ReplaceRandomMember( Control control, Resources resources )
    {
        super( control, resources );
        this.cluster = resources.cluster();
        this.backupHelper = new BackupHelper( resources );
        this.fs = resources.fileSystem();
        this.log = resources.logProvider().getLog( getClass() );
    }

    @Override
    public void doWorkOnMember( ClusterMember oldMember ) throws Exception
    {
        boolean replaceFromBackup = ThreadLocalRandom.current().nextBoolean();

        File backup = null;
        if ( replaceFromBackup )
        {
            backup = createBackupWithRetries( oldMember );
        }

        log.info( "Stopping: " + oldMember );
        oldMember.shutdown();

        ClusterMember newMember = (oldMember instanceof CoreClusterMember) ?
                cluster.newCoreMember() :
                cluster.newReadReplica();

        if ( replaceFromBackup )
        {
            log.info( "Restoring backup: " + backup.getName() + " to: " + newMember );
            restoreFromBackup( backup, fs, newMember );
            fs.deleteRecursively( backup );
        }

        log.info( "Starting: " + newMember );
        newMember.start();

        successfulReplacements++;
    }

    private File createBackupWithRetries( ClusterMember member ) throws Exception
    {
        int failureCount = 0;

        while ( true )
        {
            Optional<File> backupOpt = backupHelper.backup( member );
            if ( backupOpt.isPresent() )
            {
                return backupOpt.get();
            }
            else
            {
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
