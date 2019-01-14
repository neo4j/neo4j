/*
 * Copyright (c) 2002-2019 "Neo4j,"
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
package org.neo4j.causalclustering;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.RuleChain;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Optional;

import org.neo4j.causalclustering.backup_stores.BackupStore;
import org.neo4j.causalclustering.backup_stores.BackupStoreWithSomeData;
import org.neo4j.causalclustering.backup_stores.BackupStoreWithSomeDataButNoTransactionLogs;
import org.neo4j.causalclustering.backup_stores.EmptyBackupStore;
import org.neo4j.causalclustering.cluster_load.ClusterLoad;
import org.neo4j.causalclustering.cluster_load.NoLoad;
import org.neo4j.causalclustering.cluster_load.SmallBurst;
import org.neo4j.causalclustering.discovery.Cluster;
import org.neo4j.causalclustering.discovery.CoreClusterMember;
import org.neo4j.causalclustering.discovery.IpFamily;
import org.neo4j.causalclustering.discovery.SharedDiscoveryServiceFactory;
import org.neo4j.kernel.impl.store.format.standard.Standard;
import org.neo4j.test.rule.SuppressOutput;
import org.neo4j.test.rule.TestDirectory;
import org.neo4j.test.rule.fs.DefaultFileSystemRule;

import static java.util.Collections.emptyMap;
import static org.junit.Assert.assertFalse;
import static org.neo4j.causalclustering.BackupUtil.restoreFromBackup;
import static org.neo4j.causalclustering.discovery.Cluster.dataMatchesEventually;

@RunWith( Parameterized.class )
public class NewMemberSeedingIT
{
    @Parameterized.Parameter()
    public BackupStore seedStore;

    @Parameterized.Parameter( 1 )
    public ClusterLoad intermediateLoad;

    private SuppressOutput suppressOutput = SuppressOutput.suppressAll();
    private TestDirectory testDir = TestDirectory.testDirectory();
    private DefaultFileSystemRule fileSystemRule = new DefaultFileSystemRule();

    @Rule
    public RuleChain rules = RuleChain.outerRule( fileSystemRule ).around( testDir ).around( suppressOutput );

    private Cluster cluster;
    private FileCopyDetector fileCopyDetector;
    private File baseBackupDir;

    @Parameterized.Parameters( name = "{0} with {1}" )
    public static Iterable<Object[]> data()
    {
        return combine( stores(), loads() );
    }

    private static Iterable<Object[]> combine( Iterable<BackupStore> stores, Iterable<ClusterLoad> loads )
    {
        ArrayList<Object[]> params = new ArrayList<>();
        for ( BackupStore store : stores )
        {
            for ( ClusterLoad load : loads )
            {
                params.add( new Object[]{store, load} );
            }
        }
        return params;
    }

    private static Iterable<ClusterLoad> loads()
    {
        return Arrays.asList( new NoLoad(), new SmallBurst() );
    }

    private static Iterable<BackupStore> stores()
    {
        return Arrays.asList( new EmptyBackupStore(), new BackupStoreWithSomeData(), new BackupStoreWithSomeDataButNoTransactionLogs() );
    }

    @Before
    public void setup()
    {
        this.fileCopyDetector = new FileCopyDetector();
        cluster = new Cluster( testDir.directory( "cluster-b" ), 3, 0, new SharedDiscoveryServiceFactory(), emptyMap(), emptyMap(), emptyMap(), emptyMap(),
                Standard.LATEST_NAME, IpFamily.IPV4, false );
        baseBackupDir = testDir.directory( "backups" );
    }

    @After
    public void after()
    {
        if ( cluster != null )
        {
            cluster.shutdown();
        }
    }

    @Test
    public void shouldSeedNewMemberToCluster() throws Exception
    {
        // given
        cluster.start();

        // when
        Optional<File> backup = seedStore.generate( baseBackupDir, cluster );

        // then
        // possibly add load to cluster in between backup
        intermediateLoad.start( cluster );

        // when
        CoreClusterMember newCoreClusterMember = cluster.addCoreMemberWithId( 3 );
        if ( backup.isPresent() )
        {
            restoreFromBackup( backup.get(), fileSystemRule.get(), newCoreClusterMember );
        }

        // we want the new instance to seed from backup and not delete and re-download the store
        newCoreClusterMember.monitors().addMonitorListener( fileCopyDetector );
        newCoreClusterMember.start();

        // then
        intermediateLoad.stop();
        dataMatchesEventually( newCoreClusterMember, cluster.coreMembers() );
        assertFalse( fileCopyDetector.hasDetectedAnyFileCopied() );
    }
}
