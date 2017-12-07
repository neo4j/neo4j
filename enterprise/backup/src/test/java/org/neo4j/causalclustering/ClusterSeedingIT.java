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
package org.neo4j.causalclustering;

import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;

import java.io.File;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.neo4j.causalclustering.catchup.tx.FileCopyMonitor;
import org.neo4j.causalclustering.catchup.tx.PullRequestMonitor;
import org.neo4j.causalclustering.core.CoreGraphDatabase;
import org.neo4j.causalclustering.discovery.Cluster;
import org.neo4j.causalclustering.discovery.CoreClusterMember;
import org.neo4j.causalclustering.discovery.IpFamily;
import org.neo4j.causalclustering.discovery.SharedDiscoveryService;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.enterprise.configuration.OnlineBackupSettings;
import org.neo4j.kernel.impl.store.format.standard.Standard;
import org.neo4j.kernel.monitoring.Monitors;
import org.neo4j.restore.RestoreDatabaseCommand;
import org.neo4j.test.DbRepresentation;
import org.neo4j.test.rule.TestDirectory;
import org.neo4j.test.rule.fs.DefaultFileSystemRule;

import static java.util.Collections.emptyMap;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.neo4j.causalclustering.BackupCoreIT.backupAddress;
import static org.neo4j.causalclustering.discovery.Cluster.dataMatchesEventually;
import static org.neo4j.causalclustering.helpers.DataCreator.createEmptyNodes;
import static org.neo4j.util.TestHelpers.runBackupToolFromOtherJvmToGetExitCode;

public class ClusterSeedingIT
{
    private Cluster backupCluster;
    private Cluster cluster;
    private FileSystemAbstraction fsa;
    private DetectFileCopyMonitor detectFileCopyMonitor;
    private PullRequestMonitor pullRequestMonitor;

    @Rule
    public TestDirectory testDir = TestDirectory.testDirectory();
    @Rule
    public DefaultFileSystemRule fileSystemRule = new DefaultFileSystemRule();
    private File baseBackupDir;

    @Before
    public void setup() throws Exception
    {
        fsa = fileSystemRule.get();
        Monitors monitors = new Monitors();
        addMonitorListeners( monitors );
        backupCluster = new Cluster( testDir.directory( "cluster-for-backup" ), 3, 0,
                new SharedDiscoveryService(), emptyMap(), emptyMap(), emptyMap(), emptyMap(), Standard
                .LATEST_NAME, IpFamily.IPV4, false, new Monitors() );

        cluster = new Cluster( testDir.directory( "cluster-b" ), 3, 0,
                new SharedDiscoveryService(), emptyMap(), emptyMap(), emptyMap(), emptyMap(), Standard.LATEST_NAME,
                IpFamily.IPV4, false, monitors );

        baseBackupDir = testDir.directory( "backups" );
    }

    private void addMonitorListeners( Monitors monitors )
    {
        this.detectFileCopyMonitor = new DetectFileCopyMonitor();
        this.pullRequestMonitor = new DetectPullRequestMonitor();
        monitors.addMonitorListener( detectFileCopyMonitor );
        monitors.addMonitorListener( pullRequestMonitor );
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

    private File createBackupUsingAnotherCluster() throws Exception
    {
        backupCluster.start();
        CoreGraphDatabase db = BackupCoreIT.createSomeData( backupCluster );

        File backup = createBackup( backupCluster, "some-backup" );
        backupCluster.shutdown();

        return backup;
    }

    private File createBackup( Cluster cluster, String backupName ) throws Exception
    {
        String[] args = BackupCoreIT.backupArguments( backupAddress( cluster ), baseBackupDir, backupName );
        assertEquals( 0, runBackupToolFromOtherJvmToGetExitCode( testDir.absolutePath(), args ) );
        return new File( baseBackupDir, backupName );
    }

    @Test
    public void shouldRestoreBySeedingAllMembers() throws Throwable
    {
        // given
        File backupDir = createBackupUsingAnotherCluster();
        Config config = Config.defaults( OnlineBackupSettings.online_backup_enabled, Boolean.FALSE.toString() );
        DbRepresentation before = DbRepresentation.of( backupDir, config );

        // when
        for ( CoreClusterMember coreClusterMember : cluster.coreMembers() )
        {
            String databaseName = coreClusterMember
                    .getMemberConfig().get( GraphDatabaseSettings.active_database );
            new RestoreDatabaseCommand( fsa, backupDir, coreClusterMember.getMemberConfig(), databaseName, true )
                    .execute();
        }
        cluster.start();

        // then
        dataMatchesEventually( before, cluster.coreMembers() );
        assertFalse( detectFileCopyMonitor.fileCopyDetected.get() );
        assertTrue( pullRequestMonitor.numberOfRequests() >= 2 );
    }

    @Test
    public void shouldSeedNewMemberFromEmptyIdleCluster() throws Throwable
    {
        // given
        Monitors monitors = new Monitors();
        cluster = new Cluster( testDir.directory( "cluster-b" ), 3, 0,
                new SharedDiscoveryService(), emptyMap(), emptyMap(), emptyMap(), emptyMap(), Standard.LATEST_NAME,
                IpFamily.IPV4, false, monitors );
        cluster.start();

        // when: creating a backup
        File backupDir = createBackup( cluster, "the-backup" );
        // we are only interested in monitoring the new instance
        addMonitorListeners( monitors );

        // and: seeding new member with said backup
        CoreClusterMember newMember = cluster.addCoreMemberWithId( 3 );
        String databaseName = newMember.getMemberConfig().get( GraphDatabaseSettings.active_database );
        new RestoreDatabaseCommand( fsa, backupDir, newMember.getMemberConfig(), databaseName, true ).execute();
        newMember.start();

        // then
        dataMatchesEventually( DbRepresentation.of( newMember.database() ), cluster.coreMembers() );
        assertFalse( detectFileCopyMonitor.fileCopyDetected.get() );
        assertEquals( 1, pullRequestMonitor.numberOfRequests() );
    }

    @Test
    public void shouldSeedNewMemberFromNonEmptyIdleCluster() throws Throwable
    {
        // given
        Monitors monitors = new Monitors();
        cluster = new Cluster( testDir.directory( "cluster-b" ), 3, 0,
                new SharedDiscoveryService(), emptyMap(), emptyMap(), emptyMap(), emptyMap(), Standard.LATEST_NAME,
                IpFamily.IPV4, false, monitors );

        cluster.start();
        createEmptyNodes( cluster, 100 );

        // when: creating a backup
        File backupDir = createBackup( cluster, "the-backup" );
        // we are only interested in monitoring the new instance
        addMonitorListeners( monitors );

        // and: seeding new member with said backup
        CoreClusterMember newMember = cluster.addCoreMemberWithId( 3 );
        String databaseName = newMember.getMemberConfig().get( GraphDatabaseSettings.active_database );
        new RestoreDatabaseCommand( fsa, backupDir, newMember.getMemberConfig(), databaseName, true ).execute();
        newMember.start();

        // then
        dataMatchesEventually( DbRepresentation.of( newMember.database() ), cluster.coreMembers() );
        assertFalse( detectFileCopyMonitor.fileCopyDetected.get() );
        assertEquals( 1, pullRequestMonitor.numberOfRequests() );
    }

    @Test
    @Ignore( "need to seed all members for now" )
    public void shouldRestoreBySeedingSingleMember() throws Throwable
    {
        // given
        File backupDir = createBackupUsingAnotherCluster();
        DbRepresentation before = DbRepresentation.of( backupDir );

        // when
        fsa.copyRecursively( backupDir, cluster.getCoreMemberById( 0 ).storeDir() );
        cluster.getCoreMemberById( 0 ).start();
        Thread.sleep( 2_000 );
        cluster.getCoreMemberById( 1 ).start();
        cluster.getCoreMemberById( 2 ).start();

        // then
        dataMatchesEventually( before, cluster.coreMembers() );
    }

    private class DetectPullRequestMonitor implements PullRequestMonitor
    {

        private final AtomicLong lastPullRequest = new AtomicLong();
        private final AtomicInteger numberOfRequest = new AtomicInteger();

        @Override
        public void txPullRequest( long txId )
        {
            lastPullRequest.set( txId );
            numberOfRequest.incrementAndGet();
        }

        @Override
        public void txPullResponse( long txId )
        {
            throw new UnsupportedOperationException( "not implemented" );
        }

        @Override
        public long lastRequestedTxId()
        {
            return lastPullRequest.get();
        }

        @Override
        public long lastReceivedTxId()
        {
            throw new UnsupportedOperationException( "not implemented" );
        }

        @Override
        public long numberOfRequests()
        {
            return numberOfRequest.get();
        }
    }

    private class DetectFileCopyMonitor implements FileCopyMonitor
    {
        private final AtomicBoolean fileCopyDetected = new AtomicBoolean( false );

        @Override
        public void copyFile( File file )
        {
            fileCopyDetected.compareAndSet( false, true );
        }
    }
}
