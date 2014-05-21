package org.neo4j.io.pagecache.impl.standard;

import java.io.IOException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;

import org.neo4j.io.fs.StoreChannel;
import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.io.pagecache.PageLock;
import org.neo4j.io.pagecache.PagedFile;

import static org.neo4j.io.pagecache.impl.standard.PageTable.PinnablePage;

public class StandardPagedFile implements PagedFile
{
    static final int BEING_LOADED = -1;
    static final int NOT_LOADED = -2;
    static final Object NULL = new Object();

    private final PageTable table;
    private final PageTable.PageIO pageIO;
    private ConcurrentMap<Long, Object> addressTranslationTable = new ConcurrentHashMap<>();

    public StandardPagedFile( PageTable table, StoreChannel channel )
    {
        this.table = table;
        this.pageIO = new StandardPageIO(channel);
    }

    @Override
    public void pin( PageCursor cursor, PageLock lock, long pageId ) throws IOException
    {
        for (;;)
        {
            Object pageRef = addressTranslationTable.get( pageId );
            if ( pageRef instanceof CountDownLatch )
            {
                try
                {
                    ((CountDownLatch) pageRef).await();
                }
                catch ( InterruptedException e )
                {
                    Thread.interrupted();
                    throw new IOException( "Interrupted while waiting for page load.", e );
                }
            }
            else if ( pageRef == null || pageRef == NULL )
            {
                pageRef = new CountDownLatch( 1 );
                Object existing = addressTranslationTable.putIfAbsent( pageId, pageRef );
                if ( existing == null )
                {
                    PinnablePage page = table.load( pageIO, pageId, lock );
                    addressTranslationTable.put( pageId, page );
                    ((CountDownLatch) pageRef).countDown();
                }
            }
            else
            {
                // happy case where we have a page id
                PinnablePage page = (PinnablePage) pageRef;
                if ( page.pin( pageIO, pageId, lock ) )
                {
                    return; // yay!
                }
                addressTranslationTable.replace( pageId, page, NULL );
            }
        }
    }

    @Override
    public void unpin( PageCursor cursor )
    {

    }

    @Override
    public int pageSize()
    {
        return 0;
    }
}
