package org.neo4j.kernel.api.impl.index;

import java.io.IOException;
import java.util.List;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Fieldable;
import org.apache.lucene.search.IndexSearcher;
import org.neo4j.helpers.collection.PrefetchingIterator;
import org.neo4j.kernel.api.impl.index.bitmaps.Bitmap;
import org.neo4j.kernel.api.scan.NodeLabelRange;

public class NodeLabelRangeIterator extends PrefetchingIterator<NodeLabelRange>
{
    private final IndexSearcher searcher;
    private final BitmapDocumentFormat format;
    private int id = 0;

    public NodeLabelRangeIterator( IndexSearcher searcher, BitmapDocumentFormat format )
    {
        this.searcher = searcher;
        this.format = format;
    }

    @Override
    protected NodeLabelRange fetchNextOrNull()
    {
        while ( id < searcher.maxDoc() )
        {
            if ( searcher.getIndexReader().isDeleted( id ) )
            {
                id++;
                continue;
            }

            try
            {
                Document document = searcher.doc( id );

                List<Fieldable> fields = document.getFields();

                long[] labelIds = new long[fields.size() - 1];
                Bitmap[] bitmaps = new Bitmap[fields.size() - 1];

                int i = 0;
                long rangeId = -1;
                for ( Fieldable field : fields )
                {
                    if ( format.isRangeField( field ) )
                    {
                        rangeId = format.rangeOf( field );
                    }
                    else
                    {
                        labelIds[i] = format.labelId( field );
                        bitmaps[i] = format.readBitmap( field );
                        i++;
                    }
                }
                assert (rangeId >= 0);
                id++;
                return new LuceneNodeLabelRange( labelIds, getLongs( bitmaps, rangeId ) );
            }
            catch ( IOException e )
            {
                throw new RuntimeException( e );
            }
        }
        return null;
    }

    private long[][] getLongs( Bitmap[] bitmaps, long rangeId )
    {
        long[][] nodeIds = new long[bitmaps.length][];
        for ( int k = 0; k < nodeIds.length; k++ )
        {
            nodeIds[k] = format.bitmapFormat().convertRangeAndBitmapToArray( rangeId, bitmaps[k].bitmap() );
        }
        return nodeIds;
    }
}