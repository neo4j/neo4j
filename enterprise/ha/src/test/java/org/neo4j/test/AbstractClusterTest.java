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

import java.io.File;

import org.junit.After;
import org.junit.Before;
import org.neo4j.graphdb.factory.GraphDatabaseBuilder;
import org.neo4j.kernel.lifecycle.LifeSupport;
import org.neo4j.test.ha.ClusterManager;

public abstract class AbstractClusterTest
{
    private final File dir = TargetDirectory.forTest( getClass() ).directory( "dbs", true );
    private final LifeSupport life = new LifeSupport();
    protected ClusterManager clusterManager;
    
    @Before
    public void before() throws Exception
    {
        clusterManager = life.add( new ClusterManager( clusterOfSize( 3 ), dir, stringMap( default_timeout.name(), "1s" ) )
        {
            @Override
            protected void config( GraphDatabaseBuilder builder, String clusterName, int serverId )
            {
                super.config( builder, clusterName, serverId );
                configureClusterMember( builder, clusterName, serverId );
            }
        } );
        life.start();
    }

    protected void configureClusterMember( GraphDatabaseBuilder builder, String clusterName, int serverId )
    {
    }

    @After
    public void after() throws Exception
    {
        life.shutdown();
    }
}
