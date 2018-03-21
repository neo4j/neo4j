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
import java.util.concurrent.ThreadLocalRandom;

import org.neo4j.causalclustering.discovery.Cluster;
import org.neo4j.causalclustering.discovery.ClusterMember;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.logging.Log;

import static org.neo4j.causalclustering.BackupUtil.createBackupInProcess;
import static org.neo4j.causalclustering.BackupUtil.restoreFromBackup;

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
    protected void doWorkOnMember( ClusterMember oldMember ) throws Exception
    {
        File backupDir = null;

        String backupName = null;
        boolean replaceFromBackup = ThreadLocalRandom.current().nextBoolean();

        if ( replaceFromBackup )
        {
            backupName = "backup-" + backupNumber++;
            backupDir = createBackupInProcess( oldMember, baseBackupDir, backupName );
            log.info( "Created backup: " + backupName + " from: " + oldMember );
        }

        log.info( "Stopping: " + oldMember );
        oldMember.shutdown();

        boolean newMemberIsCore = ThreadLocalRandom.current().nextBoolean();
        ClusterMember newMember = newMemberIsCore ? cluster.newCoreMember() : cluster.newReadReplica();

        if ( backupDir != null )
        {
            log.info( "Restoring backup: " + backupName + " to: " + newMember );
            restoreFromBackup( backupDir, fs, newMember );
        }

        log.info( "Starting: " + newMember );
        newMember.start();
    }
}
