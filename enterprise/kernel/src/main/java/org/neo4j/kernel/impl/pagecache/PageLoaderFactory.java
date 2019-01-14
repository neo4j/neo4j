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
package org.neo4j.kernel.impl.pagecache;

import java.io.IOException;
import java.util.concurrent.ExecutorService;

import org.neo4j.io.fs.FileUtils;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.PagedFile;

class PageLoaderFactory
{
    private final ExecutorService executor;
    private final PageCache pageCache;

    PageLoaderFactory( ExecutorService executor, PageCache pageCache )
    {
        this.executor = executor;
        this.pageCache = pageCache;
    }

    PageLoader getLoader( PagedFile file ) throws IOException
    {
        if ( FileUtils.highIODevice( file.file().toPath(), false ) )
        {
            return new ParallelPageLoader( file, executor, pageCache );
        }
        return new SingleCursorPageLoader( file );
    }
}
