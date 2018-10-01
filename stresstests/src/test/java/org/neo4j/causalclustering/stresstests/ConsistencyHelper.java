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
import java.io.PrintStream;

import org.neo4j.causalclustering.catchup.storecopy.CopiedStoreRecovery;
import org.neo4j.causalclustering.catchup.storecopy.TemporaryStoreDirectory;
import org.neo4j.consistency.ConsistencyCheckService;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.impl.muninn.StandalonePageCacheFactory;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.scheduler.JobScheduler;
import org.neo4j.scheduler.ThreadPoolJobScheduler;

import static org.neo4j.consistency.ConsistencyCheckTool.runConsistencyCheckTool;
import static org.neo4j.graphdb.facade.GraphDatabaseDependencies.newDependencies;
import static org.neo4j.io.NullOutputStream.NULL_OUTPUT_STREAM;

class ConsistencyHelper
{
    static void assertStoreConsistent( FileSystemAbstraction fs, File storeDir ) throws Exception
    {
        File parent = storeDir.getParentFile();
        try ( JobScheduler jobScheduler = new ThreadPoolJobScheduler();
              PageCache pageCache = StandalonePageCacheFactory.createPageCache( fs, jobScheduler );
              TemporaryStoreDirectory tempStore = new TemporaryStoreDirectory( fs, pageCache, parent ) )
        {
            fs.copyRecursively( storeDir, tempStore.storeDir() );

            new CopiedStoreRecovery( Config.defaults(), newDependencies().kernelExtensions(), pageCache )
                    .recoverCopiedStore( tempStore.databaseLayout() );

            ConsistencyCheckService.Result result = runConsistencyCheckTool(
                    new String[]{storeDir.getAbsolutePath()},
                    new PrintStream( NULL_OUTPUT_STREAM ),
                    new PrintStream( NULL_OUTPUT_STREAM ) );

            if ( !result.isSuccessful() )
            {
                throw new RuntimeException( "Not consistent database in " + storeDir );
            }
        }
    }
}
