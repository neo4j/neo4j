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

import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.impl.logging.LogService;
import org.neo4j.logging.Log;

import static org.neo4j.com.storecopy.StoreUtil.cleanStoreDir;
import static org.neo4j.com.storecopy.StoreUtil.deleteRecursive;
import static org.neo4j.com.storecopy.StoreUtil.getBranchedDataRootDirectory;
import static org.neo4j.com.storecopy.StoreUtil.isBranchedDataDirectory;
import static org.neo4j.com.storecopy.StoreUtil.moveAwayDb;
import static org.neo4j.com.storecopy.StoreUtil.newBranchedDataDir;

public enum BranchedDataPolicy
{
    keep_all
            {
                @Override
                public void handle( File storeDir, PageCache pageCache, LogService logService ) throws IOException
                {
                    Log msgLog = logService.getInternalLog( getClass() );
                    File branchedDataDir = newBranchedDataDir( storeDir );
                    msgLog.debug( "Moving store from " + storeDir + " to " + branchedDataDir );
                    moveAwayDb( storeDir, branchedDataDir, pageCache );
                }
            },
    keep_last
            {
                @Override
                public void handle( File storeDir, PageCache pageCache, LogService logService ) throws IOException
                {
                    Log msgLog = logService.getInternalLog( getClass() );

                    File branchedDataDir = newBranchedDataDir( storeDir );
                    msgLog.debug( "Moving store from " + storeDir + " to " + branchedDataDir );
                    moveAwayDb( storeDir, branchedDataDir, pageCache );
                    for ( File file : getBranchedDataRootDirectory( storeDir ).listFiles() )
                    {
                        if ( isBranchedDataDirectory( file ) && !file.equals( branchedDataDir ) )
                        {
                            deleteRecursive( file, pageCache );
                        }
                    }
                }
            },
    keep_none
            {
                @Override
                public void handle( File storeDir, PageCache pageCache, LogService logService ) throws IOException
                {
                    Log msgLog = logService.getInternalLog( getClass() );
                    msgLog.debug( "Removing store  " + storeDir );
                    cleanStoreDir( storeDir, pageCache );
                }
            };

    public abstract void handle( File storeDir, PageCache pageCache, LogService msgLog ) throws IOException;
}
