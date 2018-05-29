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
package org.neo4j.causalclustering.stresstests;

import java.io.File;
import java.util.concurrent.locks.LockSupport;
import java.util.function.BooleanSupplier;

import org.neo4j.causalclustering.catchup.storecopy.CopiedStoreRecovery;
import org.neo4j.causalclustering.catchup.storecopy.TemporaryStoreDirectory;
import org.neo4j.causalclustering.discovery.Cluster;
import org.neo4j.causalclustering.discovery.ClusterMember;
import org.neo4j.consistency.ConsistencyCheckService;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.impl.muninn.StandalonePageCacheFactory;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.extension.KernelExtensions;

import static org.neo4j.consistency.ConsistencyCheckTool.runConsistencyCheckTool;

class StartStopLoad extends RepeatUntilOnSelectedMemberCallable
{
    private final FileSystemAbstraction fs;
    private final PageCache pageCache;

    StartStopLoad( FileSystemAbstraction fs, PageCache pageCache, BooleanSupplier keepGoing, Runnable onFailure,
            Cluster cluster, int numberOfCores, int numberOfEdges )
    {
        super( keepGoing, onFailure, cluster, numberOfCores, numberOfEdges );
        this.fs = fs;
        this.pageCache = pageCache;
    }

    @Override
    protected void doWorkOnMember( boolean isCore, int id )
    {
        ClusterMember member = isCore ? cluster.getCoreMemberById( id ) : cluster.getReadReplicaById( id );
        String storeDir = member.database().getStoreDir();
        KernelExtensions kernelExtensions =
                member.database().getDependencyResolver().resolveDependency( KernelExtensions.class );
        member.shutdown();
        assertStoreConsistent( storeDir, kernelExtensions );
        LockSupport.parkNanos( 5_000_000_000L );
        member.start();
    }

    private void assertStoreConsistent( String storeDir, KernelExtensions kernelExtensions )
    {
        File fromDirectory = new File( storeDir );
        File parent = fromDirectory.getParentFile();
        try ( TemporaryStoreDirectory storeDirectory = new TemporaryStoreDirectory( fs, pageCache, parent );
              PageCache pageCache = StandalonePageCacheFactory.createPageCache( fs ) )
        {
            fs.copyRecursively( fromDirectory, storeDirectory.storeDir() );
            new CopiedStoreRecovery( Config.defaults(), kernelExtensions.listFactories(),  pageCache )
                    .recoverCopiedStore( storeDirectory.storeDir() );
            ConsistencyCheckService.Result result = runConsistencyCheckTool( new String[]{storeDir} );
            if ( !result.isSuccessful() )
            {
                throw new RuntimeException( "Not consistent database in " + storeDir );
            }
        }
        catch ( Throwable e )
        {
            throw new RuntimeException( "Failed to run CC on " + storeDir, e );
        }
    }
}
