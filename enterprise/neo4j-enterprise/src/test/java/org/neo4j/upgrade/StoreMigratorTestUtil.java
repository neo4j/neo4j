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
package org.neo4j.upgrade;

import java.io.File;
import java.io.IOException;
import java.util.Map;

import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.fs.FileUtils;
import org.neo4j.kernel.impl.ha.ClusterManager;
import org.neo4j.kernel.lifecycle.LifeSupport;

import static org.neo4j.kernel.impl.ha.ClusterManager.clusterOfSize;

public class StoreMigratorTestUtil
{
    StoreMigratorTestUtil()
    {
        // no instance allowed
    }

    public static ClusterManager.ManagedCluster buildClusterWithMasterDirIn( FileSystemAbstraction fs,
            final File legacyStoreDir, LifeSupport life,
            final Map<String,String> sharedConfig ) throws Throwable
    {
        File haRootDir = new File( legacyStoreDir.getParentFile(), "ha-migration" );
        fs.deleteRecursively( haRootDir );

        ClusterManager clusterManager = new ClusterManager.Builder( haRootDir )
                .withStoreDirInitializer( ( serverId, storeDir ) ->
                {
                    if ( serverId == 1 ) // Initialize dir only for master, others will copy store from it
                    {
                        FileUtils.copyRecursively( legacyStoreDir, storeDir );
                    }
                } )
                .withCluster( clusterOfSize( 3 ) )
                .withSharedConfig( sharedConfig )
                .build();

        life.add( clusterManager );
        life.start();

        return clusterManager.getCluster();
    }
}
