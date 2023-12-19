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
