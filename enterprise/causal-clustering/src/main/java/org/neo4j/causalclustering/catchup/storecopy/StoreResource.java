package org.neo4j.causalclustering.catchup.storecopy;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.nio.channels.ReadableByteChannel;
import java.util.Optional;

import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.PagedFile;

class StoreResource implements Closeable
{
    private final File file;
    private final String path;
    private final int recordSize;
    private final PageCache pageCache;
    private final FileSystemAbstraction fs;

    private ReadableByteChannel channel;

    StoreResource( File file, String relativePath, int recordSize, PageCache pageCache, FileSystemAbstraction fs )
    {
        this.file = file;
        this.path = relativePath;
        this.recordSize = recordSize;
        this.pageCache = pageCache;
        this.fs = fs;
    }

    ReadableByteChannel open() throws IOException
    {
        Optional<PagedFile> existingMapping = pageCache.getExistingMapping( file );
        if ( existingMapping.isPresent() )
        {
            try ( PagedFile pagedFile = existingMapping.get() )
            {
                channel = pagedFile.openReadableByteChannel();
            }
        }
        else
        {
            channel = fs.open( file, "r" );
        }
        return channel;
    }

    @Override
    public void close() throws IOException
    {
        channel.close();
    }

    public String path()
    {
        return path;
    }

    int recordSize()
    {
        return recordSize;
    }

    @Override
    public String toString()
    {
        return "StoreResource{" + "path='" + path + '\'' + ", channel=" + channel + ", recordSize=" + recordSize + '}';
    }
}
