/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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
package org.neo4j.kernel.api.impl.labelscan.writer;

import org.apache.lucene.document.Document;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.TermQuery;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.Lock;

import org.neo4j.kernel.api.impl.index.collector.FirstHitCollector;
import org.neo4j.kernel.api.impl.index.partition.IndexPartition;
import org.neo4j.kernel.api.impl.index.partition.PartitionSearcher;
import org.neo4j.kernel.api.impl.labelscan.LuceneLabelScanIndex;
import org.neo4j.kernel.api.impl.labelscan.bitmaps.Bitmap;
import org.neo4j.kernel.api.impl.labelscan.storestrategy.BitmapDocumentFormat;
import org.neo4j.kernel.api.labelscan.LabelScanWriter;
import org.neo4j.kernel.api.labelscan.NodeLabelUpdate;

import static java.lang.String.format;

/**
 * Label scan store Lucene index writer implementation that supports writing into multiple partitions and creates
 * partitions on-demand if needed.
 * <p>
 * Writer chooses writable partition based on the given nodeId and configured parameter
 * {@link #MAXIMUM_PARTITION_SIZE}.
 * Additional partitions are created on-demand, if needed.
 */
public class PartitionedLuceneLabelScanWriter implements LabelScanWriter
{

    private final Integer MAXIMUM_PARTITION_SIZE =
            Integer.getInteger( "labelScanStore.maxPartitionSize", IndexWriter.MAX_DOCS );

    private final BitmapDocumentFormat format;

    private final List<NodeLabelUpdate> updates;
    private long currentRange;
    private LuceneLabelScanIndex index;

    public PartitionedLuceneLabelScanWriter( LuceneLabelScanIndex index, BitmapDocumentFormat format)
    {
        this.index = index;
        this.format = format;
        currentRange = -1;
        updates = new ArrayList<>( format.bitmapFormat().rangeSize() );
    }

    @Override
    public void write( NodeLabelUpdate update ) throws IOException
    {
        long range = format.bitmapFormat().rangeOf( update.getNodeId() );

        if ( range != currentRange )
        {
            if ( range < currentRange )
            {
                throw new IllegalArgumentException( format( "NodeLabelUpdates must be supplied in order of " +
                                                            "ascending node id. Current range:%d, node id of this " +
                                                            "update:%d",
                        currentRange, update.getNodeId() ) );
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
        index.maybeRefreshBlocking();
    }

    private Map<Long/*range*/,Bitmap> readLabelBitMapsInRange( IndexSearcher searcher, long range ) throws IOException
    {
        Map<Long/*label*/,Bitmap> fields = new HashMap<>();
        Term documentTerm = format.rangeTerm( range );
        TermQuery query = new TermQuery( documentTerm );
        FirstHitCollector hitCollector = new FirstHitCollector();
        searcher.search( query, hitCollector );
        if ( hitCollector.hasMatched() )
        {
            Document document = searcher.doc( hitCollector.getMatchedDoc() );
            for ( IndexableField field : document.getFields() )
            {
                if ( !format.isRangeOrLabelField( field ) )
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
        IndexPartition partition = getCurrentPartition();
        try ( PartitionSearcher partitionSearcher = partition.acquireSearcher() )
        {
            IndexSearcher searcher = partitionSearcher.getIndexSearcher();
            Map<Long/*label*/,Bitmap> fields = readLabelBitMapsInRange( searcher, currentRange );
            updateFields( updates, fields );

            Document document = new Document();
            format.addRangeValuesField( document, currentRange );

            for ( Map.Entry<Long/*label*/,Bitmap> field : fields.entrySet() )
            {
                // one field per label
                Bitmap value = field.getValue();
                if ( value.hasContent() )
                {
                    format.addLabelAndSearchFields( document, field.getKey(), value );
                }
            }

            if ( isEmpty( document ) )
            {
                partition.getIndexWriter().deleteDocuments( format.rangeTerm( document ) );
            }
            else
            {
                partition.getIndexWriter().updateDocument( format.rangeTerm( document ), document );
            }
            updates.clear();
        }
    }

    // since only one writer is allowed at any point in time
    // its safe to do partition allocation without any additional lock
    private IndexPartition getCurrentPartition() throws IOException
    {
        int partition = getPartitionForRange();

        while ( isNotEnoughPartitions( partition ) )
        {
            index.addNewPartition();
        }
        return index.getPartitions().get( partition );
    }

    private boolean isNotEnoughPartitions( int partition )
    {
        return index.getPartitions().size() < (partition + 1);
    }

    private int getPartitionForRange()
    {
        return Math.toIntExact( currentRange / MAXIMUM_PARTITION_SIZE );
    }

    private boolean isEmpty( Document document )
    {
        for ( IndexableField fieldable : document.getFields() )
        {
            if ( !format.isRangeOrLabelField( fieldable ) )
            {
                return false;
            }
        }
        return true;
    }

    private void updateFields( Iterable<NodeLabelUpdate> updates, Map<Long/*label*/,Bitmap> fields )
    {
        for ( NodeLabelUpdate update : updates )
        {
            clearLabels( fields, update );
            setLabels( fields, update );
        }
    }

    private void clearLabels( Map<Long,Bitmap> fields, NodeLabelUpdate update )
    {
        for ( Bitmap bitmap : fields.values() )
        {
            format.bitmapFormat().set( bitmap, update.getNodeId(), false );
        }
    }

    private void setLabels( Map<Long,Bitmap> fields, NodeLabelUpdate update )
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
