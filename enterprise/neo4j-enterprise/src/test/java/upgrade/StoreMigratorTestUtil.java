/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package upgrade;

import java.io.File;
import java.io.IOException;

import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.fs.FileUtils;
import org.neo4j.kernel.impl.ha.ClusterManager;
import org.neo4j.kernel.lifecycle.LifeSupport;

import static org.neo4j.kernel.impl.ha.ClusterManager.clusterOfSize;

public class StoreMigratorTestUtil
{
    StoreMigratorTestUtil()
    {
        // no istance allowed
    }

    public static ClusterManager.ManagedCluster buildClusterWithMasterDirIn( FileSystemAbstraction fs,
                                                                             final File legacyStoreDir,
                                                                             LifeSupport life )
            throws Throwable
    {
        File haRootDir = new File( legacyStoreDir.getParentFile(), "ha-migration" );
        fs.deleteRecursively( haRootDir );

        ClusterManager clusterManager = new ClusterManager.Builder( haRootDir )
                .withStoreDirInitializer( new ClusterManager.StoreDirInitializer()
                {
                    @Override
                    public void initializeStoreDir( int serverId, File storeDir ) throws IOException
                    {
                        if ( serverId == 1 ) // Initialize dir only for master, others will copy store from it
                        {
                            FileUtils.copyRecursively( legacyStoreDir, storeDir );
                        }
                    }
                } )
                .withProvider( clusterOfSize( 3 ) )
                .build();

        life.add( clusterManager );
        life.start();

        return clusterManager.getDefaultCluster();
    }
}
