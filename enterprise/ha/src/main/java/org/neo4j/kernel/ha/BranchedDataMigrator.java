/*
 * Copyright (c) 2002-2019 "Neo4j,"
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
package org.neo4j.kernel.ha;

import java.io.File;
import java.io.IOException;

import org.neo4j.io.fs.FileUtils;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.com.storecopy.StoreUtil;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;

public class BranchedDataMigrator extends LifecycleAdapter
{
    private final File storeDir;
    private final PageCache pageCache;

    public BranchedDataMigrator( File storeDir, PageCache pageCache )
    {
        this.storeDir = storeDir;
        this.pageCache = pageCache;
    }

    @Override
    public void start()
    {
        migrateBranchedDataDirectoriesToRootDirectory();
    }

    private void migrateBranchedDataDirectoriesToRootDirectory()
    {
        File branchedDir = StoreUtil.getBranchedDataRootDirectory( storeDir );
        branchedDir.mkdirs();
        for ( File oldBranchedDir : storeDir.listFiles() )
        {
            if ( !oldBranchedDir.isDirectory() || !oldBranchedDir.getName().startsWith( "branched-" ) )
            {
                continue;
            }

            long timestamp = 0;
            try
            {
                timestamp = Long.parseLong( oldBranchedDir.getName().substring( oldBranchedDir.getName().indexOf( '-'
                ) + 1 ) );
            }
            catch ( NumberFormatException e )
            {   // OK, it wasn't a branched directory after all.
                continue;
            }

            File targetDir = StoreUtil.getBranchedDataDirectory( storeDir, timestamp );
            try
            {
                FileUtils.moveFile( oldBranchedDir, targetDir );
                StoreUtil.moveAwayDbWithPageCache( oldBranchedDir, targetDir, pageCache, f -> true );
            }
            catch ( IOException e )
            {
                throw new RuntimeException( "Couldn't move branched directories to " + branchedDir, e );
            }
        }
    }
}
