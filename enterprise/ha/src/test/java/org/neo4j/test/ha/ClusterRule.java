/**
 * Copyright (c) 2002-2015 "Neo Technology,"
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
package org.neo4j.test.ha;

import java.io.File;
import java.util.Map;

import org.junit.rules.ExternalResource;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;

import org.neo4j.graphdb.factory.HighlyAvailableGraphDatabaseFactory;
import org.neo4j.test.TargetDirectory;

import static org.neo4j.cluster.ClusterSettings.default_timeout;
import static org.neo4j.helpers.collection.MapUtil.stringMap;
import static org.neo4j.kernel.ha.HaSettings.tx_push_factor;
import static org.neo4j.test.ha.ClusterManager.masterAvailable;

public class ClusterRule extends ExternalResource
{
    private final Class<?> testClass;

    private ClusterManager clusterManager;
    private File storeDirectory;
    private Description description;

    public ClusterRule( Class<?> testClass )
    {
        this.testClass = testClass;
    }

    public ClusterManager.ManagedCluster startCluster() throws Exception
    {
        return startCluster( new HighlyAvailableGraphDatabaseFactory(), stringMap() );
    }

    public ClusterManager.ManagedCluster startCluster(Map<String, String> config) throws Exception
    {
        return startCluster( new HighlyAvailableGraphDatabaseFactory(), config );
    }

    public ClusterManager.ManagedCluster startCluster( HighlyAvailableGraphDatabaseFactory databaseFactory )
            throws Exception
    {
        return startCluster( databaseFactory, stringMap() );
    }

    public ClusterManager.ManagedCluster startCluster( HighlyAvailableGraphDatabaseFactory databaseFactory,
                                                       Map<String, String> config ) throws Exception
    {
        config.putAll(stringMap(
                default_timeout.name(), "1s",
                tx_push_factor.name(), "0"));
        clusterManager = new ClusterManager.Builder( storeDirectory )
                .withDbFactory(databaseFactory).build();
        try
        {
            clusterManager.start();
        }
        catch ( Throwable throwable )
        {
            throw new RuntimeException( throwable );
        }
        ClusterManager.ManagedCluster cluster = clusterManager.getDefaultCluster();
        cluster.await( masterAvailable() );
        return cluster;
    }

    @Override
    public Statement apply( Statement base, Description description )
    {
        this.description = description;
        return super.apply( base, description );
    }


    @Override
    protected void before() throws Throwable
    {
        this.storeDirectory = TargetDirectory.forTest( testClass ).directoryForDescription( description );
    }

    @Override
    protected void after()
    {
        try
        {
            if ( clusterManager != null )
            {
                clusterManager.stop();
            }
        }
        catch ( Throwable throwable )
        {
            throwable.printStackTrace();
        }
    }
}
