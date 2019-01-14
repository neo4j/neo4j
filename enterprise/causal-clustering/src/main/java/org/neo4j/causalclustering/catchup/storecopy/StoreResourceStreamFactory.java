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
import java.util.function.Supplier;

import org.neo4j.cursor.RawCursor;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.NeoStoreDataSource;
import org.neo4j.storageengine.api.StoreFileMetadata;

import static org.neo4j.io.fs.FileUtils.relativePath;

public class StoreResourceStreamFactory
{
    private final PageCache pageCache;
    private final FileSystemAbstraction fs;
    private final Supplier<NeoStoreDataSource> dataSourceSupplier;

    public StoreResourceStreamFactory( PageCache pageCache, FileSystemAbstraction fs, Supplier<NeoStoreDataSource> dataSourceSupplier )
    {
        this.pageCache = pageCache;
        this.fs = fs;
        this.dataSourceSupplier = dataSourceSupplier;
    }

    RawCursor<StoreResource,IOException> create() throws IOException
    {
        NeoStoreDataSource dataSource = dataSourceSupplier.get();

        File storeDir = dataSource.getStoreDir();
        ResourceIterator<StoreFileMetadata> files = dataSource.listStoreFiles( false );

        return new RawCursor<StoreResource,IOException>()
        {
            private StoreResource resource;

            @Override
            public StoreResource get()
            {
                return resource;
            }

            @Override
            public boolean next() throws IOException
            {
                if ( !files.hasNext() )
                {
                    resource = null;
                    return false;
                }

                StoreFileMetadata md = files.next();

                resource = new StoreResource( md.file(), relativePath( storeDir, md.file() ), md.recordSize(), pageCache, fs );
                return true;
            }

            @Override
            public void close()
            {
                files.close();
            }
        };
    }
}
