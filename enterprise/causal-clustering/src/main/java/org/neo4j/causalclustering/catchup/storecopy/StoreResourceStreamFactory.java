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
            public void close() throws IOException
            {
            }
        };
    }
}
