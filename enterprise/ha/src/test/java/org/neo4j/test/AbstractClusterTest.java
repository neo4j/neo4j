/**
 * Copyright (c) 2002-2013 "Neo Technology,"
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
package org.neo4j.test;

import static org.neo4j.cluster.ClusterSettings.default_timeout;
import static org.neo4j.helpers.collection.MapUtil.stringMap;
import static org.neo4j.test.ha.ClusterManager.clusterOfSize;
import static org.neo4j.test.ha.ClusterManager.masterAvailable;

import java.io.File;

import org.junit.After;
import org.junit.Before;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.factory.GraphDatabaseBuilder;
import org.neo4j.kernel.lifecycle.LifeSupport;
import org.neo4j.test.ha.ClusterManager;
import org.neo4j.test.ha.ClusterManager.ManagedCluster;
import org.neo4j.test.ha.ClusterManager.Provider;

public abstract class AbstractClusterTest
{
    private final File dir = TargetDirectory.forTest( getClass() ).directory( "dbs", true );
    protected LifeSupport life = new LifeSupport();
    private final Provider provider;
    protected ClusterManager clusterManager;
    protected ManagedCluster cluster;
    
    protected AbstractClusterTest( ClusterManager.Provider provider )
    {
        this.provider = provider;
    }
    
    protected AbstractClusterTest()
    {
        this( clusterOfSize( 3 ) );
    }
    
    @Before
    public void before() throws Exception
    {
        clusterManager = life.add( new ClusterManager( provider, dir, stringMap( default_timeout.name(), "1s" ) )
        {
            @Override
            protected void config( GraphDatabaseBuilder builder, String clusterName, int serverId )
            {
                super.config( builder, clusterName, serverId );
                configureClusterMember( builder, clusterName, serverId );
            }
            
            @Override
            protected void insertInitialData( GraphDatabaseService db, String name, int serverId )
            {
                super.insertInitialData( db, name, serverId );
                insertClusterMemberInitialData( db, name, serverId );
            }
        } );
        life.start();
        cluster = clusterManager.getDefaultCluster();
        cluster.await( masterAvailable() );
    }

    protected void configureClusterMember( GraphDatabaseBuilder builder, String clusterName, int serverId )
    {
    }

    protected void insertClusterMemberInitialData( GraphDatabaseService db, String name, int serverId )
    {
    }
    
    @After
    public void after() throws Exception
    {
        life.shutdown();
    }
}
