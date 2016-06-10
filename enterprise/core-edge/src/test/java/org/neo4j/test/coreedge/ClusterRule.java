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
package org.neo4j.test.coreedge;

import org.junit.rules.ExternalResource;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;

import org.neo4j.coreedge.discovery.Cluster;
import org.neo4j.coreedge.discovery.DiscoveryServiceFactory;
import org.neo4j.coreedge.discovery.SharedDiscoveryService;
import org.neo4j.graphdb.config.Setting;
import org.neo4j.kernel.impl.store.format.standard.StandardV3_0;
import org.neo4j.test.rule.TargetDirectory;

import java.io.File;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.function.IntFunction;

import static org.neo4j.helpers.collection.MapUtil.stringMap;

public class ClusterRule extends ExternalResource implements ClusterBuilder<ClusterRule>
{
    private final TargetDirectory.TestDirectory testDirectory;
    private File clusterDirectory;
    private Cluster cluster;

    private int noCoreServers = 3;
    private int noEdgeServers = 3;
    private DiscoveryServiceFactory factory = new SharedDiscoveryService();
    private Map<String,String> coreParams = stringMap();
    private Map<String,IntFunction<String>> instanceCoreParams = new HashMap<>();
    private Map<String,String> edgeParams = stringMap();
    private Map<String,IntFunction<String>> instanceEdgeParams = new HashMap<>();
    private String recordFormat = StandardV3_0.NAME;

    public ClusterRule( Class<?> testClass )
    {
        this.testDirectory = TargetDirectory.testDirForTest( testClass );
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
            try
            {
                cluster.shutdown();
            }
            catch ( Throwable e )
            {
                throw new RuntimeException( e );
            }
        }
    }

    /**
     * Starts cluster with the configuration provided at instantiation time. This method will not return until the
     * cluster is up and all members report each other as available.
     */
    public Cluster startCluster() throws Exception
    {
        if ( cluster == null )
        {
            cluster = new Cluster( clusterDirectory, noCoreServers, noEdgeServers, factory, coreParams,
                    instanceCoreParams, edgeParams, instanceEdgeParams, recordFormat );
            cluster.start();
        }
        cluster.awaitLeader();
        return cluster;
    }

    public TargetDirectory.TestDirectory testDirectory()
    {
        return testDirectory;
    }

    @Override
    public ClusterRule withNumberOfCoreServers( int noCoreServers )
    {
        this.noCoreServers = noCoreServers;
        return  this;
    }

    @Override
    public ClusterRule withNumberOfEdgeServers( int noEdgeServers )
    {
        this.noEdgeServers = noEdgeServers;
        return this;
    }

    @Override
    public ClusterRule withDiscoveryServiceFactory( DiscoveryServiceFactory factory )
    {
        this.factory = factory;
        return this;
    }

    @Override
    public ClusterRule withSharedCoreParams( Map<String,String> params )
    {
        this.coreParams.putAll( params );
        return this;
    }

    @Override
    public ClusterRule withSharedCoreParam( Setting<?> key, String value )
    {
        this.coreParams.put( key.name(), value );
        return this;
    }

    @Override
    public ClusterRule withInstanceCoreParams( Map<String,IntFunction<String>> params )
    {
        this.instanceCoreParams.putAll( params );
        return this;
    }

    @Override
    public ClusterRule withInstanceCoreParam( Setting<?> key, IntFunction<String> valueFunction )
    {
        this.instanceCoreParams.put( key.name(), valueFunction );
        return this;
    }

    @Override
    public ClusterRule withSharedEdgeParams( Map<String,String> params )
    {
        this.edgeParams.putAll( params );
        return this;
    }

    @Override
    public ClusterRule withSharedEdgeParam( Setting<?> key, String value )
    {
        this.edgeParams.put( key.name(), value );
        return this;
    }

    @Override
    public ClusterRule withInstanceEdgeParams( Map<String,IntFunction<String>> params )
    {
        this.instanceEdgeParams.putAll( params );
        return this;
    }

    @Override
    public ClusterRule withInstanceEdgeParam( Setting<?> key, IntFunction<String> valueFunction )
    {
        this.instanceEdgeParams.put( key.name(), valueFunction );
        return this;
    }

    @Override
    public ClusterRule withRecordFormat( String recordFormat )
    {
        this.recordFormat = recordFormat;
        return this;
    }
}
