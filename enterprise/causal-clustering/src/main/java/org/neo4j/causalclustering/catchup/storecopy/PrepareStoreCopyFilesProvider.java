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
package org.neo4j.causalclustering.catchup.storecopy;

import java.util.function.Supplier;

import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.NeoStoreDataSource;

public class PrepareStoreCopyFilesProvider
{
    private final PageCache pageCache;
    private final FileSystemAbstraction fileSystemAbstraction;

    public PrepareStoreCopyFilesProvider( PageCache pageCache, FileSystemAbstraction fileSystemAbstraction )
    {
        this.pageCache = pageCache;
        this.fileSystemAbstraction = fileSystemAbstraction;
    }

    PrepareStoreCopyFiles prepareStoreCopyFiles( NeoStoreDataSource neoStoreDataSource )
    {
        return new PrepareStoreCopyFiles( neoStoreDataSource, pageCache, fileSystemAbstraction );
    }
}
