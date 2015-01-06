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
import java.io.IOException;
import java.net.URISyntaxException;
import java.util.Map;

import org.apache.commons.io.FileUtils;

import org.neo4j.cluster.InstanceId;
import org.neo4j.cluster.client.Clusters;
import org.neo4j.cluster.client.Clusters.Cluster;
import org.neo4j.kernel.lifecycle.LifeSupport;

public class OnlineBackupClusterManager extends ClusterManager
{
    private final String pathToBackupStore;

    public OnlineBackupClusterManager( Provider clustersProvider, File root, Map<String,String> commonConfig )
    {
        super( clustersProvider, root, commonConfig );
        pathToBackupStore = root.getAbsolutePath();
    }

    @Override
    public void start() throws Throwable
    {
        Clusters clusters = clustersProvider.clusters();
        life = new LifeSupport();
        // Started so instances added here will be started immediately, and in case of exceptions they can be
        // shutdown() or stop()ped properly
        life.start();
        for ( int i = 0; i < clusters.getClusters().size(); i++ )
        {
            Clusters.Cluster cluster = clusters.getClusters().get( i );
            ManagedCluster managedCluster = new OnlineBackupManagedCluster( cluster ); // use a new manager
            clusterMap.put( cluster.getName(), managedCluster );
            life.add( managedCluster );
        }
    }

    public class OnlineBackupManagedCluster extends ManagedCluster
    {
        OnlineBackupManagedCluster( Cluster spec ) throws URISyntaxException
        {
            super( spec );
        }

        @Override
        protected void startMember( InstanceId serverId ) throws URISyntaxException
        {
            String to = new File( new File( pathToBackupStore ).getParent(), "instance" + serverId ).getPath();
            try
            {
                // delete store if already exists
                FileUtils.deleteDirectory( new File( to ) );
                // copy store to another new path
                copyStore( pathToBackupStore, to );
            }
            catch ( IOException e )
            {
                System.out.println( "Failed to copy the instance with the backup db: failed to copy db from "
                        + pathToBackupStore + " to " + to );
                e.printStackTrace();
                System.exit( 1 );
            }
            // start the member with the copied store
            startMember( serverId, to );
        }

        private void copyStore( String from, String to ) throws IOException
        {
            FileUtils.copyDirectory( new File( from ), new File( to ) );
        }
    }
}
