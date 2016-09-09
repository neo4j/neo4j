package org.neo4j.index;

import java.io.IOException;
import java.util.List;

import org.neo4j.index.btree.Node;
import org.neo4j.io.pagecache.PageCursor;

public interface Seeker
{
    /**
     * Cursor will be moved from page.
     * @param cursor        {@link PageCursor} pinned to page with node (internal or leaf)
     * @param resultList    {@link java.util.List} where found results will be stored
     * @throws IOException  on cursor failure
     */
    void seek( PageCursor cursor, Node node, List<SCResult> resultList ) throws IOException;

    public abstract class CommonSeeker implements Seeker
    {

        // TODO: A lot of time is spent in the seek method, both for seek and scan. Can we make it faster?
        // TODO: Maybe with binary search in IndexSearch.
        public void seek( PageCursor cursor, Node node, List<SCResult> resultList ) throws IOException
        {
            if ( node.isInternal( cursor ) )
            {
                seekInternal( cursor, node, resultList );
            }
            else if ( node.isLeaf( cursor ) )
            {
                seekLeaf( cursor, node, resultList );
            }
            else
            {
                throw new IllegalStateException( "node reported type other than internal or leaf" );
            }
        }

        protected abstract void seekLeaf( PageCursor cursor, Node node, List<SCResult> resultList ) throws IOException;

        protected abstract void seekInternal( PageCursor cursor, Node node, List<SCResult> resultList ) throws IOException;
    }
}
