/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
