/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
