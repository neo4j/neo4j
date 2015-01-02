/**
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

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Fieldable;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopDocs;

import org.neo4j.kernel.api.impl.index.bitmaps.Bitmap;
import org.neo4j.kernel.api.labelscan.NodeLabelUpdate;
import org.neo4j.unsafe.batchinsert.LabelScanWriter;

public class LuceneLabelScanWriter implements LabelScanWriter
{
    private final LabelScanStorageStrategy.StorageService storage;
    private final BitmapDocumentFormat format;
    private final IndexSearcher searcher;

    private List<NodeLabelUpdate> updates;
    private long currentRange;

    public LuceneLabelScanWriter( LabelScanStorageStrategy.StorageService storage,
                                  BitmapDocumentFormat format )
    {
        this.storage = storage;
        this.format = format;
        currentRange = -1;
        updates = new ArrayList<>( format.bitmapFormat().rangeSize() );
        searcher = storage.acquireSearcher();
    }

    @Override
    public void write( NodeLabelUpdate update ) throws IOException
    {
        long range = format.bitmapFormat().rangeOf( update.getNodeId() );

        if ( range != currentRange )
        {
            if ( range < currentRange )
            {
                throw new IllegalArgumentException( "NodeLabelUpdates must be supplied in order of ascending node id" );
            }

            flush();
            currentRange = range;
        }

        updates.add( update );
    }

    @Override
    public void close() throws IOException
    {
        flush();
        storage.releaseSearcher( searcher );
        storage.refreshSearcher();
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


    private void flush() throws IOException
    {
        if ( currentRange < 0 )
        {
            return;
        }

        Map<Long/*label*/, Bitmap> fields = readLabelBitMapsInRange( searcher, currentRange );
        updateFields( updates, fields );

        Document document = new Document();
        document.add( format.rangeField( currentRange ) );

        for ( Map.Entry<Long/*label*/, Bitmap> field : fields.entrySet() )
        {
            // one field per label
            Bitmap value = field.getValue();
            if ( value.hasContent() )
            {
                format.addLabelField( document, field.getKey(), value );
            }
        }

        if ( isEmpty( document ) )
        {
            storage.deleteDocuments( format.rangeTerm( document ) );
        }
        else
        {
            storage.updateDocument( format.rangeTerm( document ), document );
        }
        updates.clear();
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

    private void updateFields( Iterable<NodeLabelUpdate> updates, Map<Long/*label*/, Bitmap> fields )
    {
        for ( NodeLabelUpdate update : updates )
        {
            clearLabels( fields, update );
            setLabels( fields, update );
        }
    }

    private void clearLabels( Map<Long, Bitmap> fields, NodeLabelUpdate update )
    {
        for ( Bitmap bitmap : fields.values() )
        {
            format.bitmapFormat().set( bitmap, update.getNodeId(), false );
        }
    }

    private void setLabels( Map<Long, Bitmap> fields, NodeLabelUpdate update )
    {
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
