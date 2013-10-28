package org.neo4j.kernel.api.impl.index;

import java.util.Iterator;

import org.apache.lucene.search.IndexSearcher;
import org.neo4j.kernel.api.scan.NodeLabelRange;
import org.neo4j.kernel.api.scan.NodeRangeReader;

public class LuceneNodeRangeReader implements NodeRangeReader
{
    private final IndexSearcher searcher;
    private final BitmapDocumentFormat format;

    public LuceneNodeRangeReader( IndexSearcher searcher, BitmapDocumentFormat format )
    {
        this.searcher = searcher;
        this.format = format;
    }

    @Override
    public Iterator<NodeLabelRange> iterator()
    {
        return new NodeLabelRangeIterator( searcher, format);
    }

    @Override
    public void close() throws Exception
    {
        searcher.close();
    }
}
