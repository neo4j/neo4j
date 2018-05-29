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
package org.neo4j.causalclustering.backup;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.io.File;

import org.neo4j.backup.OnlineBackupSettings;
import org.neo4j.causalclustering.discovery.Cluster;
import org.neo4j.causalclustering.discovery.CoreClusterMember;
import org.neo4j.test.DbRepresentation;
import org.neo4j.test.causalclustering.ClusterRule;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.neo4j.backup.OnlineBackupCommandIT.runBackupToolFromOtherJvmToGetExitCode;
import static org.neo4j.causalclustering.helpers.BackupUtil.backupAddress;
import static org.neo4j.causalclustering.helpers.BackupUtil.backupArguments;
import static org.neo4j.causalclustering.helpers.BackupUtil.getConfig;
import static org.neo4j.causalclustering.helpers.DataCreator.createSomeData;

public class BackupCoreIT
{
    @Rule
    public ClusterRule clusterRule = new ClusterRule( getClass() )
            .withNumberOfCoreMembers( 3 )
            .withNumberOfReadReplicas( 0 )
            .withInstanceCoreParam( OnlineBackupSettings.online_backup_server, serverId -> ":" + (8000 + serverId) );

    private Cluster cluster;
    private File backupsDir;

    @Before
    public void setup() throws Exception
    {
        backupsDir = clusterRule.testDirectory().cleanDirectory( "backups" );
        cluster = clusterRule.startCluster();
    }

    @Test
    public void makeSureBackupCanBePerformedFromAnyInstance() throws Throwable
    {
        for ( CoreClusterMember db : cluster.coreMembers() )
        {
            // Run backup
            DbRepresentation beforeChange = DbRepresentation.of( createSomeData( cluster ) );
            String[] args = backupArguments( backupAddress( db.database() ), backupsDir, "" + db.serverId() );
            assertEquals( 0, runBackupToolFromOtherJvmToGetExitCode(
                    clusterRule.clusterDirectory(), args ) );

            // Add some new data
            DbRepresentation afterChange = DbRepresentation.of( createSomeData( cluster ) );

            // Verify that old data is back
            DbRepresentation backupRepresentation = DbRepresentation.of( new File( backupsDir, "" + db.serverId() ),
                    getConfig() );
            assertEquals( beforeChange, backupRepresentation );
            assertNotEquals( backupRepresentation, afterChange );
        }
    }
}
