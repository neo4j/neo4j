package org.neo4j.index;

import java.io.IOException;
import java.util.List;

import org.neo4j.index.btree.BTreeNode;
import org.neo4j.io.pagecache.PageCursor;

public interface Seeker
{
    /**
     * Cursor will be moved from page.
     * @param cursor        {@link PageCursor} pinned to page with node (internal or leaf)
     * @param resultList    {@link java.util.List} where found results will be stored
     * @throws IOException  on cursor failure
     */
    void seek( PageCursor cursor, BTreeNode BTreeNode, List<SCResult> resultList ) throws IOException;

    public abstract class CommonSeeker implements Seeker
    {

        // TODO: A lot of time is spent in the seek method, both for seek and scan. Can we make it faster?
        // TODO: Maybe with binary search in IndexSearch.
        public void seek( PageCursor cursor, BTreeNode BTreeNode, List<SCResult> resultList ) throws IOException
        {
            if ( BTreeNode.isInternal( cursor ) )
            {
                seekInternal( cursor, BTreeNode, resultList );
            }
            else if ( BTreeNode.isLeaf( cursor ) )
            {
                seekLeaf( cursor, BTreeNode, resultList );
            }
            else
            {
                throw new IllegalStateException( "node reported type other than internal or leaf" );
            }
        }

        protected abstract void seekLeaf( PageCursor cursor, BTreeNode BTreeNode, List<SCResult> resultList ) throws IOException;

        protected abstract void seekInternal( PageCursor cursor, BTreeNode BTreeNode, List<SCResult> resultList ) throws IOException;
    }
}
