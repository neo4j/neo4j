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
package org.neo4j.causalclustering.backup;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.File;
import java.util.Arrays;
import java.util.Map;
import java.util.function.IntFunction;

import org.neo4j.backup.OnlineBackupSettings;
import org.neo4j.causalclustering.backup.util.BackupStore;
import org.neo4j.causalclustering.backup.util.BackupStoreWithSomeData;
import org.neo4j.causalclustering.backup.util.BackupStoreWithSomeDataButNoTransactionLogs;
import org.neo4j.causalclustering.backup.util.EmptyBackupStore;
import org.neo4j.causalclustering.discovery.Cluster;
import org.neo4j.causalclustering.discovery.CoreClusterMember;
import org.neo4j.causalclustering.discovery.SharedDiscoveryService;
import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.kernel.impl.store.format.standard.StandardV3_0;
import org.neo4j.test.DbRepresentation;
import org.neo4j.test.rule.TestDirectory;

import static java.util.Collections.emptyMap;
import static java.util.Collections.singletonMap;
import static org.neo4j.causalclustering.helpers.BackupUtil.restoreFromBackup;
import static org.neo4j.causalclustering.discovery.Cluster.dataMatchesEventually;

@RunWith( Parameterized.class )
public class ClusterSeedingIT
{
    private Cluster backupCluster;
    private Cluster cluster;
    private DefaultFileSystemAbstraction fsa = new DefaultFileSystemAbstraction();

    @Parameterized.Parameters( name = "{0}" )
    public static Iterable<BackupStore> data() throws Exception
    {
        return stores();
    }

    private static Iterable<BackupStore> stores()
    {
        return Arrays.asList(
                new EmptyBackupStore(),
                new BackupStoreWithSomeData(),
                new BackupStoreWithSomeDataButNoTransactionLogs() );
    }

    @Parameterized.Parameter()
    public BackupStore initialStore;

    @Rule
    public TestDirectory testDir = TestDirectory.testDirectory();
    private File baseBackupDir;

    @Before
    public void setup() throws Exception
    {
        backupCluster = new Cluster( testDir.directory( "cluster-for-backup" ), 3, 0,
                new SharedDiscoveryService(), emptyMap(), backupParams(), emptyMap(), emptyMap(), StandardV3_0.NAME );

        cluster = new Cluster( testDir.directory( "cluster-b" ), 3, 0,
                new SharedDiscoveryService(), emptyMap(), emptyMap(), emptyMap(), emptyMap(), StandardV3_0.NAME );

        baseBackupDir = testDir.directory( "backups" );
    }

    private Map<String,IntFunction<String>> backupParams()
    {
        return singletonMap(
                OnlineBackupSettings.online_backup_server.name(),
                serverId -> (":" + (8000 + serverId)) );
    }

    @After
    public void after() throws Exception
    {
        if ( backupCluster != null )
        {
            backupCluster.shutdown();
        }
        if ( cluster != null )
        {
            cluster.shutdown();
        }
    }

    @Test
    public void shouldSeedNewCluster() throws Exception
    {
        // given
        backupCluster.start();
        File backup = initialStore.get( baseBackupDir, backupCluster );

        backupCluster.shutdown();

        for ( CoreClusterMember coreClusterMember : cluster.coreMembers() )
        {
            restoreFromBackup( backup, fsa, coreClusterMember );
        }

        // when
        cluster.start();

        //then
        dataMatchesEventually( DbRepresentation.of( backup ), cluster.coreMembers() );
    }
}
