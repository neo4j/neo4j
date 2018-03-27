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
package org.neo4j.causalclustering;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.RuleChain;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.File;
import java.util.Optional;

import org.neo4j.causalclustering.backup_stores.BackupStore;
import org.neo4j.causalclustering.backup_stores.BackupStoreWithSomeData;
import org.neo4j.causalclustering.backup_stores.BackupStoreWithSomeDataButNoTransactionLogs;
import org.neo4j.causalclustering.backup_stores.EmptyBackupStore;
import org.neo4j.causalclustering.backup_stores.NoStore;
import org.neo4j.causalclustering.discovery.Cluster;
import org.neo4j.causalclustering.discovery.CoreClusterMember;
import org.neo4j.causalclustering.discovery.IpFamily;
import org.neo4j.causalclustering.discovery.SharedDiscoveryServiceFactory;
import org.neo4j.kernel.impl.store.format.standard.Standard;
import org.neo4j.test.DbRepresentation;
import org.neo4j.test.rule.SuppressOutput;
import org.neo4j.test.rule.TestDirectory;
import org.neo4j.test.rule.fs.DefaultFileSystemRule;

import static java.util.Collections.emptyMap;
import static org.junit.Assert.assertEquals;
import static org.neo4j.causalclustering.BackupUtil.restoreFromBackup;
import static org.neo4j.causalclustering.discovery.Cluster.dataMatchesEventually;

@RunWith( Parameterized.class )
public class ClusterSeedingIT
{
    @Parameterized.Parameter()
    public BackupStore initialStore;

    @Parameterized.Parameter( 1 )
    public boolean shouldStoreCopy;

    private SuppressOutput suppressOutput = SuppressOutput.suppressAll();
    private TestDirectory testDir = TestDirectory.testDirectory();
    private DefaultFileSystemRule fileSystemRule = new DefaultFileSystemRule();

    @Rule
    public RuleChain rules = RuleChain.outerRule( fileSystemRule ).around( testDir ).around( suppressOutput );

    private Cluster backupCluster;
    private Cluster cluster;
    private FileCopyDetector fileCopyDetector;
    private File baseBackupDir;

    @Parameterized.Parameters( name = "{0}" )
    public static Object[][] data()
    {
        return new Object[][]{{new NoStore(), true}, {new EmptyBackupStore(), false}, {new BackupStoreWithSomeData(), false},
                {new BackupStoreWithSomeDataButNoTransactionLogs(), false}};
    }

    @Before
    public void setup()
    {
        this.fileCopyDetector = new FileCopyDetector();
        backupCluster = new Cluster( testDir.directory( "cluster-for-backup" ), 3, 0,
                new SharedDiscoveryServiceFactory(), emptyMap(), emptyMap(), emptyMap(), emptyMap(), Standard
                .LATEST_NAME, IpFamily.IPV4, false );

        cluster = new Cluster( testDir.directory( "cluster-b" ), 3, 0,
                new SharedDiscoveryServiceFactory(), emptyMap(), emptyMap(), emptyMap(), emptyMap(), Standard.LATEST_NAME,
                IpFamily.IPV4, false );

        baseBackupDir = testDir.directory( "backups" );
    }

    @After
    public void after()
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
        Optional<File> backup = initialStore.generate( baseBackupDir, backupCluster );
        backupCluster.shutdown();

        if ( backup.isPresent() )
        {
            for ( CoreClusterMember coreClusterMember : cluster.coreMembers() )
            {
                restoreFromBackup( backup.get(), fileSystemRule.get(), coreClusterMember );
            }
        }

        // we want the cluster to seed from backup. No instance should delete and re-copy the store.
        cluster.coreMembers().forEach( ccm -> ccm.monitors().addMonitorListener( fileCopyDetector ) );

        // when
        cluster.start();

        // then
        if ( backup.isPresent() )
        {
            dataMatchesEventually( DbRepresentation.of( backup.get() ), cluster.coreMembers() );
        }
        assertEquals( shouldStoreCopy, fileCopyDetector.hasDetectedAnyFileCopied() );
    }
}
