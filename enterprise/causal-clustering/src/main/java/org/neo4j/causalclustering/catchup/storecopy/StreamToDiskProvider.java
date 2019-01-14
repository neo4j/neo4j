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
package org.neo4j.causalclustering.catchup.storecopy;

import java.io.File;
import java.io.IOException;
import java.nio.file.StandardOpenOption;

import org.neo4j.causalclustering.catchup.tx.FileCopyMonitor;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.PagedFile;
import org.neo4j.kernel.impl.store.StoreType;
import org.neo4j.kernel.monitoring.Monitors;

public class StreamToDiskProvider implements StoreFileStreamProvider
{
    private final File storeDir;
    private final FileSystemAbstraction fs;
    private final PageCache pageCache;
    private final FileCopyMonitor fileCopyMonitor;

    StreamToDiskProvider( File storeDir, FileSystemAbstraction fs, PageCache pageCache, Monitors monitors )
    {
        this.storeDir = storeDir;
        this.fs = fs;
        this.pageCache = pageCache;
        this.fileCopyMonitor = monitors.newMonitor( FileCopyMonitor.class );
    }

    @Override
    public StoreFileStream acquire( String destination, int requiredAlignment ) throws IOException
    {
        File fileName = new File( storeDir, destination );
        fs.mkdirs( fileName.getParentFile() );
        fileCopyMonitor.copyFile( fileName );
        if ( !pageCache.fileSystemSupportsFileOperations() && StoreType.canBeManagedByPageCache( destination ) )
        {
            int filePageSize = pageCache.pageSize() - pageCache.pageSize() % requiredAlignment;
            PagedFile pagedFile = pageCache.map( fileName, filePageSize, StandardOpenOption.CREATE );
            return StreamToDisk.fromPagedFile( pagedFile );
        }
        else
        {
            return StreamToDisk.fromFile( fs, fileName );
        }
    }
}
