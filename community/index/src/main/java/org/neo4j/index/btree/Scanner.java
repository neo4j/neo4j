package org.neo4j.index.btree;

import java.io.IOException;
import java.util.List;

import org.neo4j.index.SCKey;
import org.neo4j.index.SCResult;
import org.neo4j.index.SCValue;
import org.neo4j.index.Seeker;
import org.neo4j.io.pagecache.PageCursor;

public class Scanner extends Seeker.CommonSeeker
{

    @Override
    protected void seekLeaf( PageCursor cursor, Node node, List<SCResult> resultList ) throws IOException
    {
        while ( true )
        {
            int keyCount = node.keyCount( cursor );
            for ( int i = 0; i < keyCount; i++ )
            {
                long[] key = node.keyAt( cursor, i );
                long[] value = node.valueAt( cursor, i );
                resultList.add( new SCResult( new SCKey( key[0], key[1] ), new SCValue( value[0], value[1] ) ) );
            }
            long rightSibling = node.rightSibling( cursor );
            if ( rightSibling == Node.NO_NODE_FLAG )
            {
                break;
            }
            cursor.next( rightSibling );
        }
    }

    @Override
    protected void seekInternal( PageCursor cursor, Node node, List<SCResult> resultList ) throws IOException
    {
        cursor.next( node.childAt( cursor, 0 ) );
        seek( cursor, node, resultList );
    }
}
