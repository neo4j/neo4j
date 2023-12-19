/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * Neo4j Sweden Software License, as found in the associated LICENSE.txt
 * file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * Neo4j Sweden Software License for more details.
 */
package org.neo4j.causalclustering.stresstests;

import java.io.File;
import java.io.PrintStream;

import org.neo4j.causalclustering.catchup.storecopy.CopiedStoreRecovery;
import org.neo4j.causalclustering.catchup.storecopy.TemporaryStoreDirectory;
import org.neo4j.causalclustering.discovery.ClusterMember;
import org.neo4j.consistency.ConsistencyCheckService;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.impl.muninn.StandalonePageCacheFactory;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.extension.KernelExtensions;
import org.neo4j.logging.Log;

import static org.neo4j.consistency.ConsistencyCheckTool.runConsistencyCheckTool;
import static org.neo4j.io.NullOutputStream.NULL_OUTPUT_STREAM;

class StartStopRandomCore extends RepeatOnRandomCore
{
    private final FileSystemAbstraction fs;
    private final PageCache pageCache;
    private final Log log;

    StartStopRandomCore( Control control, Resources resources )
    {
        super( control, resources );
        this.fs = resources.fileSystem();
        this.pageCache = resources.pageCache();
        this.log = resources.logProvider().getLog( getClass() );
    }

    @Override
    protected void doWorkOnMember( ClusterMember member ) throws InterruptedException
    {
        File storeDir = member.database().getStoreDir();
        KernelExtensions kernelExtensions = member.database().getDependencyResolver().resolveDependency( KernelExtensions.class );
        log.info( "Stopping: " + member );
        member.shutdown();
        assertStoreConsistent( storeDir, kernelExtensions );
        Thread.sleep( 5000 );
        log.info( "Starting: " + member );
        member.start();
    }

    private void assertStoreConsistent( File storeDir, KernelExtensions kernelExtensions )
    {
        File parent = storeDir.getParentFile();
        try ( TemporaryStoreDirectory storeDirectory = new TemporaryStoreDirectory( fs, pageCache, parent );
              PageCache pageCache = StandalonePageCacheFactory.createPageCache( fs ) )
        {
            fs.copyRecursively( storeDir, storeDirectory.storeDir() );
            new CopiedStoreRecovery( Config.defaults(), kernelExtensions.listFactories(),  pageCache )
                    .recoverCopiedStore( storeDirectory.storeDir() );
            ConsistencyCheckService.Result result = runConsistencyCheckTool( new String[]{storeDir.getAbsolutePath()},
                    new PrintStream( NULL_OUTPUT_STREAM ), new PrintStream( NULL_OUTPUT_STREAM ) );
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
