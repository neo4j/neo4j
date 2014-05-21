package org.neo4j.io.pagecache.impl.standard;

import org.neo4j.io.pagecache.PageLock;
import org.neo4j.io.pagecache.impl.common.OffsetTrackingCursor;

public class StandardPageCursor extends OffsetTrackingCursor
{
    private PageLock lockTypeHeld;

    public PageTable.PinnablePage page()
    {
        return (PageTable.PinnablePage)page;
    }

    /** The type of lock the cursor holds */
    public PageLock lockType()
    {
        return lockTypeHeld;
    }

    public void reset( PageTable.PinnablePage page, PageLock lockTypeHeld )
    {
        this.lockTypeHeld = lockTypeHeld;
        super.reset( page );

    }
}
