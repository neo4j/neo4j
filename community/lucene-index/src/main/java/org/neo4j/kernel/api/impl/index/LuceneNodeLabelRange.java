package org.neo4j.kernel.api.impl.index;

import org.neo4j.kernel.api.scan.NodeLabelRange;

public class LuceneNodeLabelRange implements NodeLabelRange
{
    private final static long[] NO_NODES = new long[0];

    private final long[] labelIds;
    private final long[][] nodeIds;

    public LuceneNodeLabelRange( long[] labelIds, long[][] nodeIds )
    {
        this.labelIds = labelIds;
        this.nodeIds = nodeIds;
    }

    @Override
    public long[] labels()
    {
        return labelIds;
    }

    @Override
    public long[] nodes( long labelId )
    {
        for ( int i = 0; i < labelIds.length; i++ )
        {
            if ( labelId == labelIds[i] )
            {
                return nodeIds[i];
            }
        }
        return NO_NODES;
    }
}
