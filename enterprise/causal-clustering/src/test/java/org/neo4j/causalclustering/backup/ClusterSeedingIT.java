/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.neo4j.backup.OnlineBackupSettings;
import org.neo4j.causalclustering.core.CoreGraphDatabase;
import org.neo4j.causalclustering.discovery.Cluster;
import org.neo4j.causalclustering.discovery.SharedDiscoveryService;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.impl.store.format.standard.Standard;
import org.neo4j.test.DbRepresentation;
import org.neo4j.test.rule.TestDirectory;
import org.neo4j.test.rule.fs.DefaultFileSystemRule;

import java.io.File;
import java.util.HashMap;
import java.util.function.IntFunction;

import static org.junit.Assert.assertEquals;
import static org.neo4j.backup.OnlineBackupCommandIT.runBackupToolFromOtherJvmToGetExitCode;
import static org.neo4j.causalclustering.backup.BackupCoreIT.backupAddress;
import static org.neo4j.causalclustering.discovery.Cluster.dataMatchesEventually;
import static org.neo4j.helpers.collection.MapUtil.stringMap;

public class ClusterSeedingIT
{
    private Cluster backupCluster;
    private Cluster cluster;
    private FileSystemAbstraction fsa;

    @Rule
    public TestDirectory testDir = TestDirectory.testDirectory();
    @Rule
    public DefaultFileSystemRule fileSystemRule = new DefaultFileSystemRule();

    @Before
    public void setup() throws Exception
    {
        fsa = fileSystemRule.get();
        HashMap<String,IntFunction<String>> instanceCoreParams = new HashMap<>();
        instanceCoreParams.put(
                OnlineBackupSettings.online_backup_server.name(),
                serverId -> (":" + (8000 + serverId)) );

        backupCluster =
                new Cluster( testDir.directory( "cluster-for-backup" ), 3, 0, new SharedDiscoveryService(), stringMap(),
                        instanceCoreParams, stringMap(), new HashMap<>(), Standard.LATEST_NAME );

        cluster = new Cluster( testDir.directory( "cluster-b" ), 3, 0, new SharedDiscoveryService(), stringMap(),
                new HashMap<>(), stringMap(), new HashMap<>(), Standard.LATEST_NAME );
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

    private File createBackupUsingCoreCluster() throws Exception
    {
        File backupDir = testDir.directory( "backups" );

        backupCluster.start();
        CoreGraphDatabase db = BackupCoreIT.createSomeData( backupCluster );

        String[] args = BackupCoreIT.backupArguments( backupAddress( db ), backupDir, "core" );
        assertEquals( 0, runBackupToolFromOtherJvmToGetExitCode( args ) );
        backupCluster.shutdown();

        return new File( backupDir, "core" );
    }

    @Test
    public void shouldRestoreBySeedingAllMembers() throws Throwable
    {
        // given
        File backupDir = createBackupUsingCoreCluster();
        DbRepresentation before = DbRepresentation.of( backupDir );

        // when
        fsa.copyRecursively( backupDir, cluster.getCoreMemberById( 0 ).storeDir() );
        fsa.copyRecursively( backupDir, cluster.getCoreMemberById( 1 ).storeDir() );
        fsa.copyRecursively( backupDir, cluster.getCoreMemberById( 2 ).storeDir() );
        cluster.start();

        // then
        dataMatchesEventually( before, cluster.coreMembers() );
    }

    @Test
    @Ignore( "need to seed all members for now" )
    public void shouldRestoreBySeedingSingleMember() throws Throwable
    {
        // given
        File backupDir = createBackupUsingCoreCluster();
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
}
