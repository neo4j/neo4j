package org.neo4j.io.pagecache.impl.standard;

import java.nio.ByteBuffer;

import org.neo4j.io.pagecache.PageLock;
import org.neo4j.io.pagecache.impl.common.Page;

public interface PageTable
{
    /**
     * Load a new page into the table. This does not guarantee avoiding duplicate
     * pages loaded into the cache, it is up to the callee to ensure pages do not get
     * duplicated into the table.
     *
     * The page returned is pre-locked with the lock specified in the call.
     */
    PinnablePage load( PageIO io, long pageId, PageLock lock );

    interface PageIO
    {
        void read( long pageId, ByteBuffer into );
        void write( long pageId, ByteBuffer from );
    }

    interface PinnablePage extends Page
    {
        boolean pin( PageIO assertIO, long assertPageId, PageLock lock );
    }
}
