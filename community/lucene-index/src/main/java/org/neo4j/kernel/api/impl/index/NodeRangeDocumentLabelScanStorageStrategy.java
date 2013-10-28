/**
 * Copyright (c) 2002-2013 "Neo Technology,"
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

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Fieldable;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopDocs;
import org.neo4j.kernel.api.impl.index.bitmaps.Bitmap;
import org.neo4j.kernel.api.impl.index.bitmaps.BitmapFormat;
import org.neo4j.kernel.api.scan.NodeLabelUpdate;
import org.neo4j.kernel.api.scan.NodeRangeReader;
import org.neo4j.kernel.impl.api.PrimitiveLongIterator;

import static org.neo4j.helpers.collection.IteratorUtil.flatten;

/**
 * {@link org.neo4j.kernel.api.scan.LabelScanStore} implemented using Lucene. There's only one big index for all labels
 * because the Lucene document structure handles that quite efficiently.
 *
 * With {@link BitmapFormat#_32 32bit bitmaps} it would look as follows:
 *
 * { // document for nodes  0-31
 * range: 0
 * 4: [0000 0001][0000 0001][0000 0001][0000 0001] -- Node#0, Node#8, Node16, and Node#24 have Label#4
 * 7: [1000 0000][1000 0000][1000 0000][1000 0000] -- Node#7, Node#15, Node#23, and Node#31 have Label#7
 * 9: [0000 0001][0000 0000][0000 0000][1000 0000] -- Node#7, and Node#24 have Label#9
 * }
 * { // document for nodes 32-63
 * range: 1
 * 3: [0000 0000][0001 0000][0000 0000][0000 0000] -- Node#52 has Label#3
 * }
 *
 * i.e. each document represents a range of nodes, and in each document there is a field for each label that is present
 * on any of the nodes in the range. The value of that field is a bitmap with a bit set for each node in the range that
 * has that particular label.
 */
public class NodeRangeDocumentLabelScanStorageStrategy implements LabelScanStorageStrategy
{
    private static final int DOCUMENT_BATCH_SIZE = 32, CHANGES_BATCH_SIZE = 256;

    // This must be high to avoid to many calls to the lucene searcher. Tweak using LabelScanBenchmark
    private static final int RANGES_PER_PAGE = 4096;
    private final BitmapDocumentFormat format;

    public NodeRangeDocumentLabelScanStorageStrategy()
    {
        this( BitmapDocumentFormat._32 );
    }

    NodeRangeDocumentLabelScanStorageStrategy( BitmapDocumentFormat format )
    {
        this.format = format;
    }

    @Override
    public String toString()
    {
        return String.format( "%s{%s}", getClass().getSimpleName(), format );
    }

    @Override
    public PrimitiveLongIterator nodesWithLabel( IndexSearcher searcher, int labelId )
    {
        return flatten(
                new PageOfRangesIterator( format, searcher, RANGES_PER_PAGE, format.labelQuery( labelId ), labelId ) );
    }

    @Override
    public NodeRangeReader newNodeLabelReader( final IndexSearcher searcher )
    {
        return new LuceneNodeRangeReader( searcher, format );
    }

    @Override
    public void applyUpdates( StorageService storage, Iterator<NodeLabelUpdate> updates ) throws IOException
    {
        Map<Long, List<NodeLabelUpdate>> rangedUpdates = new HashMap<>();
        for ( int size = 0; updates.hasNext(); size++ )
        {
            NodeLabelUpdate update = updates.next();
            Long range = format.bitmapFormat().rangeOf( update.getNodeId() );
            List<NodeLabelUpdate> updateList = rangedUpdates.get( range );
            if ( updateList == null )
            {
                rangedUpdates.put( range, updateList = new ArrayList<>( 8 ) );
            }
            updateList.add( update );
            if ( rangedUpdates.size() >= DOCUMENT_BATCH_SIZE // number of documents to create exceed threshold
                 || size >= CHANGES_BATCH_SIZE ) // number of updates exceed threshold
            {
                flush( storage, rangedUpdates );
                rangedUpdates.clear();
                size = 0;
            }
        }
        if ( !rangedUpdates.isEmpty() )
        {
            flush( storage, rangedUpdates );
        }
    }

    private void flush( StorageService storage, Map<Long/*range*/, List<NodeLabelUpdate>> updates ) throws IOException
    {
        for ( Document document : updatedDocuments( storage, updates ) )
        {
            if ( isEmpty( document ) )
            {
                storage.deleteDocuments( format.rangeTerm( document ) );
            }
            else
            {
                storage.updateDocument( format.rangeTerm( document ), document );
            }
        }
        storage.refreshSearcher();
    }

    private boolean isEmpty( Document document )
    {
        for ( Fieldable fieldable : document.getFields() )
        {
            if ( !format.isRangeField( fieldable ) )
            {
                return false;
            }
        }
        return true;
    }

    private List<Document> updatedDocuments( StorageService storage, Map<Long/*range*/, List<NodeLabelUpdate>> updates )
            throws IOException
    {
        List<Document> updatedDocuments = new ArrayList<>();
        IndexSearcher searcher = storage.acquireSearcher();
        try
        {
            for ( Map.Entry<Long/*range*/, List<NodeLabelUpdate>> update : updates.entrySet() )
            {
                Map<Long/*label*/, Bitmap> fields = readLabelBitMapsInRange( searcher, update.getKey() );
                updateFields( update.getValue(), fields );
                // one document per range
                Document document = new Document();
                document.add( format.rangeField( update.getKey() ) );
                for ( Map.Entry<Long/*label*/, Bitmap> field : fields.entrySet() )
                {
                    // one field per label
                    Bitmap value = field.getValue();
                    if ( value.hasContent() )
                    {
                        format.addLabelField( document, field.getKey(), value );
                    }
                }
                updatedDocuments.add( document );
            }
        }
        finally
        {
            storage.releaseSearcher( searcher );
        }
        return updatedDocuments;
    }

    private void updateFields( Iterable<NodeLabelUpdate> updates, Map<Long/*label*/, Bitmap> fields )
    {
        for ( NodeLabelUpdate update : updates )
        {
            for ( Bitmap bitmap : fields.values() )
            {
                format.bitmapFormat().set( bitmap, update.getNodeId(), false );
            }
            for ( long label : update.getLabelsAfter() )
            {
                Bitmap bitmap = fields.get( label );
                if ( bitmap == null )
                {
                    fields.put( label, bitmap = new Bitmap() );
                }
                format.bitmapFormat().set( bitmap, update.getNodeId(), true );
            }
        }
    }

    private Map<Long/*range*/, Bitmap> readLabelBitMapsInRange( IndexSearcher searcher, long range ) throws IOException
    {
        Map<Long/*label*/, Bitmap> fields = new HashMap<>();
        Term documentTerm = format.rangeTerm( range );
        TopDocs docs = searcher.search( new TermQuery( documentTerm ), 1 );
        if ( docs != null && docs.totalHits != 0 )
        {
            Document document = searcher.doc( docs.scoreDocs[0].doc );
            for ( Fieldable field : document.getFields() )
            {
                if ( !format.isRangeField( field ) )
                {
                    Long label = Long.valueOf( field.name() );
                    fields.put( label, format.readBitmap( field ) );
                }
            }
        }
        return fields;
    }

}
