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
package org.neo4j.causalclustering.scenarios;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.io.File;

import org.neo4j.causalclustering.discovery.Cluster;
import org.neo4j.causalclustering.discovery.IpFamily;
import org.neo4j.causalclustering.discovery.SharedDiscoveryServiceFactory;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.configuration.Settings;
import org.neo4j.kernel.impl.enterprise.configuration.OnlineBackupSettings;
import org.neo4j.kernel.impl.store.format.highlimit.HighLimit;
import org.neo4j.test.DbRepresentation;
import org.neo4j.test.rule.TestDirectory;
import org.neo4j.test.rule.fs.DefaultFileSystemRule;

import static java.util.Collections.emptyMap;
import static org.neo4j.causalclustering.discovery.Cluster.dataMatchesEventually;

public class ClusterCommunityToEnterpriseIT
{
    private Cluster cluster;
    private FileSystemAbstraction fsa;

    @Rule
    public TestDirectory testDir = TestDirectory.testDirectory();
    @Rule
    public DefaultFileSystemRule fileSystemRule = new DefaultFileSystemRule();

    @Before
    public void setup()
    {
        fsa = fileSystemRule.get();

        cluster = new Cluster( testDir.directory( "cluster" ), 3, 0,
                new SharedDiscoveryServiceFactory(), emptyMap(), emptyMap(), emptyMap(), emptyMap(), HighLimit.NAME,
                IpFamily.IPV4, false );
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
    public void shouldRestoreBySeedingAllMembers() throws Throwable
    {
        // given
        File storeDir = testDir.makeGraphDbDir();
        GraphDatabaseService database = new GraphDatabaseFactory().newEmbeddedDatabaseBuilder( storeDir )
                .setConfig( GraphDatabaseSettings.allow_upgrade, Settings.TRUE )
                .setConfig( GraphDatabaseSettings.record_format, HighLimit.NAME )
                .setConfig( OnlineBackupSettings.online_backup_enabled, Boolean.FALSE.toString() )
                .newGraphDatabase();
        database.shutdown();
        Config config = Config.defaults( OnlineBackupSettings.online_backup_enabled, Settings.FALSE );
        DbRepresentation before = DbRepresentation.of( storeDir, config );

        // when
        fsa.copyRecursively( storeDir, cluster.getCoreMemberById( 0 ).storeDir() );
        fsa.copyRecursively( storeDir, cluster.getCoreMemberById( 1 ).storeDir() );
        fsa.copyRecursively( storeDir, cluster.getCoreMemberById( 2 ).storeDir() );
        cluster.start();

        // then
        dataMatchesEventually( before, cluster.coreMembers() );
    }
}
