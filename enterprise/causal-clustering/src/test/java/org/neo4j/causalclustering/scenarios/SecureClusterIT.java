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
package org.neo4j.causalclustering.scenarios;

import org.junit.After;
import org.junit.Rule;
import org.junit.Test;

import java.io.File;
import java.util.Map;

import org.neo4j.causalclustering.core.CausalClusteringSettings;
import org.neo4j.causalclustering.discovery.Cluster;
import org.neo4j.causalclustering.discovery.CoreClusterMember;
import org.neo4j.causalclustering.discovery.HazelcastDiscoveryServiceFactory;
import org.neo4j.causalclustering.discovery.IpFamily;
import org.neo4j.causalclustering.discovery.ReadReplica;
import org.neo4j.graphdb.Node;
import org.neo4j.kernel.configuration.ssl.SslPolicyConfig;
import org.neo4j.kernel.impl.store.format.standard.Standard;
import org.neo4j.kernel.monitoring.Monitors;
import org.neo4j.ssl.SslResourceBuilder;
import org.neo4j.test.rule.TestDirectory;
import org.neo4j.test.rule.fs.DefaultFileSystemRule;

import static java.util.Collections.emptyMap;
import static org.neo4j.causalclustering.discovery.Cluster.dataMatchesEventually;
import static org.neo4j.graphdb.Label.label;
import static org.neo4j.helpers.collection.MapUtil.stringMap;

public class SecureClusterIT
{
    @Rule
    public TestDirectory testDir = TestDirectory.testDirectory();

    @Rule
    public DefaultFileSystemRule fsRule = new DefaultFileSystemRule();

    private Cluster cluster;

    @After
    public void cleanup() throws Exception
    {
        if ( cluster != null )
        {
            cluster.shutdown();
        }
    }

    @Test
    public void shouldReplicateInSecureCluster() throws Exception
    {
        // given
        String sslPolicyName = "cluster";
        SslPolicyConfig policyConfig = new SslPolicyConfig( sslPolicyName );

        Map<String,String> coreParams = stringMap(
                CausalClusteringSettings.ssl_policy.name(), sslPolicyName,
                policyConfig.base_directory.name(), "certificates/cluster"
        );
        Map<String,String> readReplicaParams = stringMap(
                CausalClusteringSettings.ssl_policy.name(), sslPolicyName,
                policyConfig.base_directory.name(), "certificates/cluster"
        );

        int noOfCoreMembers = 3;
        int noOfReadReplicas = 3;

        cluster = new Cluster( testDir.absolutePath(), noOfCoreMembers, noOfReadReplicas,
                new HazelcastDiscoveryServiceFactory(), coreParams, emptyMap(), readReplicaParams,
                emptyMap(), Standard.LATEST_NAME, IpFamily.IPV4, false );

        // install the cryptographic objects for each core
        for ( CoreClusterMember core : cluster.coreMembers() )
        {
            File homeDir = cluster.getCoreMemberById( core.serverId() ).homeDir();
            File baseDir = new File( homeDir, "certificates/cluster" );
            fsRule.mkdirs( new File( baseDir, "trusted" ) );
            fsRule.mkdirs( new File( baseDir, "revoked" ) );

            int keyId = core.serverId();
            SslResourceBuilder.caSignedKeyId( keyId )
                    .trustSignedByCA().install( baseDir );
        }

        // install the cryptographic objects for each read replica
        for ( ReadReplica replica : cluster.readReplicas() )
        {
            File homeDir = cluster.getReadReplicaById( replica.serverId() ).homeDir();
            File baseDir = new File( homeDir, "certificates/cluster" );
            fsRule.mkdirs( new File( baseDir, "trusted" ) );
            fsRule.mkdirs( new File( baseDir, "revoked" ) );

            int keyId = replica.serverId() + noOfCoreMembers;
            SslResourceBuilder.caSignedKeyId( keyId )
                    .trustSignedByCA().install( baseDir );
        }

        // when
        cluster.start();

        CoreClusterMember leader = cluster.coreTx( ( db, tx ) ->
        {
            Node node = db.createNode( label( "boo" ) );
            node.setProperty( "foobar", "baz_bat" );
            tx.success();
        } );

        // then
        dataMatchesEventually( leader, cluster.coreMembers() );
        dataMatchesEventually( leader, cluster.readReplicas() );
    }
}
