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
package org.neo4j.test.causalclustering;

import org.junit.rules.ExternalResource;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;

import java.io.File;
import java.util.HashMap;
import java.util.Map;
import java.util.function.IntFunction;

import org.neo4j.causalclustering.discovery.Cluster;
import org.neo4j.causalclustering.discovery.DiscoveryServiceFactory;
import org.neo4j.causalclustering.discovery.IpFamily;
import org.neo4j.causalclustering.discovery.SharedDiscoveryService;
import org.neo4j.graphdb.config.Setting;
import org.neo4j.kernel.impl.store.format.standard.Standard;
import org.neo4j.test.rule.TestDirectory;

import static org.neo4j.causalclustering.discovery.IpFamily.IPV4;
import static org.neo4j.helpers.collection.MapUtil.stringMap;

public class ClusterRule extends ExternalResource
{
    private final TestDirectory testDirectory;
    private File clusterDirectory;
    private Cluster cluster;

    private int noCoreMembers = 3;
    private int noReadReplicas = 3;
    private DiscoveryServiceFactory discoveryServiceFactory = new SharedDiscoveryService();
    private Map<String,String> coreParams = stringMap();
    private Map<String,IntFunction<String>> instanceCoreParams = new HashMap<>();
    private Map<String,String> readReplicaParams = stringMap();
    private Map<String,IntFunction<String>> instanceReadReplicaParams = new HashMap<>();
    private String recordFormat = Standard.LATEST_NAME;
    private IpFamily ipFamily = IPV4;
    private boolean useWildcard = false;

    public ClusterRule( Class<?> testClass )
    {
        this.testDirectory = TestDirectory.testDirectory( testClass );
    }

    @Override
    public Statement apply( final Statement base, final Description description )
    {
        Statement testMethod = new Statement()
        {
            @Override
            public void evaluate() throws Throwable
            {
                // If this is used as class rule then getMethodName() returns null, so use
                // getClassName() instead.
                String name =
                        description.getMethodName() != null ? description.getMethodName() : description.getClassName();
                clusterDirectory = testDirectory.directory( name );
                base.evaluate();
            }
        };

        Statement testMethodWithBeforeAndAfter = super.apply( testMethod, description );

        return testDirectory.apply( testMethodWithBeforeAndAfter, description );
    }

    @Override
    protected void after()
    {
        if ( cluster != null )
        {
            cluster.shutdown();
        }
    }

    /**
     * Starts cluster with the configuration provided at instantiation time. This method will not return until the
     * cluster is up and all members report each other as available.
     */
    public Cluster startCluster() throws Exception
    {
        createCluster();
        cluster.start();
        cluster.awaitLeader();
        return cluster;
    }

    public Cluster createCluster() throws Exception
    {
        if ( cluster == null )
        {
            cluster = new Cluster( clusterDirectory, noCoreMembers, noReadReplicas, discoveryServiceFactory, coreParams,
                    instanceCoreParams, readReplicaParams, instanceReadReplicaParams, recordFormat, ipFamily, useWildcard );
        }

        return cluster;
    }

    public TestDirectory testDirectory()
    {
        return testDirectory;
    }

    public File clusterDirectory()
    {
        return clusterDirectory;
    }

    public ClusterRule withNumberOfCoreMembers( int noCoreMembers )
    {
        this.noCoreMembers = noCoreMembers;
        return this;
    }

    public ClusterRule withNumberOfReadReplicas( int noReadReplicas )
    {
        this.noReadReplicas = noReadReplicas;
        return this;
    }

    public ClusterRule withDiscoveryServiceFactory( DiscoveryServiceFactory factory )
    {
        this.discoveryServiceFactory = factory;
        return this;
    }

    public ClusterRule withSharedCoreParams( Map<String,String> params )
    {
        this.coreParams.putAll( params );
        return this;
    }

    public ClusterRule withSharedCoreParam( Setting<?> key, String value )
    {
        this.coreParams.put( key.name(), value );
        return this;
    }

    public ClusterRule withInstanceCoreParams( Map<String,IntFunction<String>> params )
    {
        this.instanceCoreParams.putAll( params );
        return this;
    }

    public ClusterRule withInstanceCoreParam( Setting<?> key, IntFunction<String> valueFunction )
    {
        this.instanceCoreParams.put( key.name(), valueFunction );
        return this;
    }

    public ClusterRule withSharedReadReplicaParams( Map<String,String> params )
    {
        this.readReplicaParams.putAll( params );
        return this;
    }

    public ClusterRule withSharedReadReplicaParam( Setting<?> key, String value )
    {
        this.readReplicaParams.put( key.name(), value );
        return this;
    }

    public ClusterRule withInstanceReadReplicaParams( Map<String,IntFunction<String>> params )
    {
        this.instanceReadReplicaParams.putAll( params );
        return this;
    }

    public ClusterRule withInstanceReadReplicaParam( Setting<?> key, IntFunction<String> valueFunction )
    {
        this.instanceReadReplicaParams.put( key.name(), valueFunction );
        return this;
    }

    public ClusterRule withRecordFormat( String recordFormat )
    {
        this.recordFormat = recordFormat;
        return this;
    }

    public ClusterRule withClusterDirectory( File clusterDirectory )
    {
        this.clusterDirectory = clusterDirectory;
        return this;
    }

    public ClusterRule withIpFamily( IpFamily ipFamily )
    {
        this.ipFamily = ipFamily;
        return this;
    }

    public ClusterRule useWildcard( boolean useWildcard )
    {
        this.useWildcard = useWildcard;
        return this;
    }
}
