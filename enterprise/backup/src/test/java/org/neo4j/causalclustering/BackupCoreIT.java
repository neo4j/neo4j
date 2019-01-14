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

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.neo4j.causalclustering.core.CoreGraphDatabase;
import org.neo4j.causalclustering.core.consensus.roles.Role;
import org.neo4j.causalclustering.discovery.Cluster;
import org.neo4j.causalclustering.discovery.CoreClusterMember;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.helpers.collection.MapUtil;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.enterprise.configuration.OnlineBackupSettings;
import org.neo4j.kernel.impl.store.format.standard.Standard;
import org.neo4j.ports.allocation.PortAuthority;
import org.neo4j.test.DbRepresentation;
import org.neo4j.test.causalclustering.ClusterRule;
import org.neo4j.test.rule.SuppressOutput;

import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.neo4j.graphdb.Label.label;
import static org.neo4j.test.assertion.Assert.assertEventually;
import static org.neo4j.util.TestHelpers.runBackupToolFromOtherJvmToGetExitCode;

public class BackupCoreIT
{
    @Rule
    public final SuppressOutput suppressOutput = SuppressOutput.suppressAll();
    @Rule
    public final ClusterRule clusterRule = new ClusterRule()
            .withNumberOfCoreMembers( 3 )
            .withNumberOfReadReplicas( 0 );

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
            String[] args = backupArguments( backupAddress( cluster ), backupsDir, "" + db.serverId() );
            assertEventually( () -> runBackupToolFromOtherJvmToGetExitCode( clusterRule.clusterDirectory(), args ), equalTo( 0 ), 5, TimeUnit.SECONDS );

            // Add some new data
            DbRepresentation afterChange = DbRepresentation.of( createSomeData( cluster ) );

            // Verify that old data is back
            DbRepresentation backupRepresentation = DbRepresentation.of( new File( backupsDir, "" + db.serverId() ), getConfig() );
            assertEquals( beforeChange, backupRepresentation );
            assertNotEquals( backupRepresentation, afterChange );
        }
    }

    static CoreGraphDatabase createSomeData( Cluster cluster ) throws Exception
    {
        return cluster.coreTx( ( db, tx ) ->
        {
            Node node = db.createNode( label( "boo" ) );
            node.setProperty( "foobar", "baz_bat" );
            tx.success();
        } ).database();
    }

    static String backupAddress( Cluster cluster )
    {
        return cluster.getMemberWithRole( Role.LEADER ).settingValue( "causal_clustering.transaction_listen_address" );
    }

    static String[] backupArguments( String from, File backupsDir, String name )
    {
        List<String> args = new ArrayList<>();
        args.add( "--from=" + from );
        args.add( "--cc-report-dir=" + backupsDir );
        args.add( "--backup-dir=" + backupsDir );
        args.add( "--protocol=catchup" );
        args.add( "--name=" + name );
        return args.toArray( new String[args.size()] );
    }

    static Config getConfig()
    {
        Map<String, String> config = MapUtil.stringMap(
                GraphDatabaseSettings.record_format.name(), Standard.LATEST_NAME,
                OnlineBackupSettings.online_backup_server.name(), "127.0.0.1:" + PortAuthority.allocatePort()
        );

        return Config.defaults( config );
    }
}
