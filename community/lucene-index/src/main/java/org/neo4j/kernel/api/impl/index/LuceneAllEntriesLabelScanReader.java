/*
 * Copyright (c) 2002-2015 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.kernel.api.impl.index;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import org.apache.lucene.document.Document;
import org.apache.lucene.index.IndexableField;

import org.neo4j.kernel.api.direct.AllEntriesLabelScanReader;
import org.neo4j.kernel.api.direct.BoundedIterable;
import org.neo4j.kernel.api.direct.NodeLabelRange;
import org.neo4j.kernel.api.impl.index.bitmaps.Bitmap;

public class LuceneAllEntriesLabelScanReader implements AllEntriesLabelScanReader
{
    private final BitmapDocumentFormat format;
    private final BoundedIterable<Document> documents;

    public LuceneAllEntriesLabelScanReader( BoundedIterable<Document> documents, BitmapDocumentFormat format )
    {
        this.documents = documents;
        this.format = format;
    }

    @Override
    public Iterator<NodeLabelRange> iterator()
    {
        final Iterator<Document> iterator = documents.iterator();
        return new Iterator<NodeLabelRange>()
        {
            private int id = 0;

            public boolean hasNext()
            {
                return iterator.hasNext();
            }

            public LuceneNodeLabelRange next()
            {
                return parse( id++, iterator.next() );
            }

            public void remove()
            {
                iterator.remove();
            }
        };
    }

    @Override
    public void close() throws Exception
    {
        documents.close();
    }

    @Override
    public long maxCount()
    {
        return documents.maxCount();
    }

    private LuceneNodeLabelRange parse( int id, Document document )
    {
        List<IndexableField> fields = document.getFields();

        int expectedLabelFields = fields.size() - 1;
        long[] scratchLabelIds = new long[expectedLabelFields];
        Bitmap[] scratchBitmaps = new Bitmap[expectedLabelFields];

        int i = 0;
        long rangeId = -1;
        for ( IndexableField field : fields )
        {
            if ( format.isRangeField( field ) )
            {
                rangeId = format.rangeOf( field );
            }
            else if ( format.isLabelBitmapField( field ) )
            {
                scratchLabelIds[i] = format.labelId( field );
                scratchBitmaps[i] = format.readBitmap( field );
                i++;
            }
        }
        assert (rangeId >= 0);

        final long[] labelIds;
        final Bitmap[] bitmaps;
        if (i < expectedLabelFields)
        {
            labelIds = Arrays.copyOf( scratchLabelIds, i );
            bitmaps = Arrays.copyOf( scratchBitmaps, i );
        }
        else
        {
            labelIds = scratchLabelIds;
            bitmaps = scratchBitmaps;
        }

        return LuceneNodeLabelRange.fromBitmapStructure( id, labelIds, getLongs( bitmaps, rangeId ) );
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
