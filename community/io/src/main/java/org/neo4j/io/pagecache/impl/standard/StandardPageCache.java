package org.neo4j.io.pagecache.impl.standard;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.fs.StoreChannel;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.io.pagecache.PagedFile;
import org.neo4j.io.pagecache.impl.common.OffsetTrackingCursor;

/**
 * Your average run-of-the-mill page cache.
 */
public class StandardPageCache implements PageCache
{
    private final FileSystemAbstraction fs;
    private final Map<File, StandardPagedFile> pagedFiles = new HashMap<>();
    private final PageTable table = null;

    public StandardPageCache( FileSystemAbstraction fs )
    {
        this.fs = fs;
    }

    @Override
    public synchronized PagedFile map( File file, int pageSize, int transitionalPeriodRecordSize ) throws IOException
    {
        StandardPagedFile pagedFile = pagedFiles.get( file );
        if( pagedFile == null)
        {
            StoreChannel channel = fs.open( file, "rw" );
            pagedFile = new StandardPagedFile( table, channel );
            pagedFiles.put( file, pagedFile );
        }
        return pagedFile;
    }

    @Override
    public PageCursor newCursor()
    {
        return new OffsetTrackingCursor();
    }

    @Override
    public void close()
    {

    }
}
