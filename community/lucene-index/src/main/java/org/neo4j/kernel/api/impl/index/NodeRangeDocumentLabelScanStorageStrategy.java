/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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

import org.apache.lucene.document.Fieldable;
import org.apache.lucene.document.NumericField;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.SearcherManager;
import org.apache.lucene.search.TopDocs;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.locks.Lock;

import org.neo4j.collection.primitive.PrimitiveLongIterator;
import org.neo4j.kernel.api.direct.AllEntriesLabelScanReader;
import org.neo4j.kernel.api.impl.index.bitmaps.BitmapFormat;
import org.neo4j.unsafe.batchinsert.LabelScanWriter;

import static org.neo4j.collection.primitive.PrimitiveLongCollections.concat;
import static org.neo4j.helpers.collection.IteratorUtil.emptyIterator;

/**
 * {@link org.neo4j.kernel.api.labelscan.LabelScanStore} implemented using Lucene. There's only one big index for all labels
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
        return concat(
                new PageOfRangesIterator( format, searcher, RANGES_PER_PAGE, format.labelQuery( labelId ), labelId ) );
    }

    @Override
    public AllEntriesLabelScanReader newNodeLabelReader( SearcherManager searcherManager )
    {
        return new LuceneAllEntriesLabelScanReader( new LuceneAllDocumentsReader(
                new LuceneIndexAccessor.LuceneReferenceManager.Wrap<>( searcherManager ) ), format );
    }

    @Override
    public Iterator<Long> labelsForNode( IndexSearcher searcher, long nodeId )
    {
        try
        {
            TopDocs topDocs = searcher.search( format.rangeQuery( format.bitmapFormat().rangeOf( nodeId ) ), 1 );

            if ( topDocs.scoreDocs.length < 1 )
            {
                return emptyIterator();
            }
            else if ( topDocs.scoreDocs.length > 1 )
            {
                throw new RuntimeException( "This label scan store seems to contain an incorrect number of entries ("
                        + topDocs.scoreDocs.length + ")" );
            }

            int doc = topDocs.scoreDocs[0].doc;

            List<Long> labels = new ArrayList<>();

            for ( Fieldable fields : searcher.doc( doc ).getFields() )
            {
                if ( "range".equals( fields.name() ) )
                {
                    continue;
                }

                if ( fields instanceof NumericField )
                {
                    NumericField labelField = (NumericField) fields;
                    Long bitmap = Long.decode( labelField.stringValue() );
                    if ( format.bitmapFormat().hasLabel( bitmap, nodeId ) )
                    {
                        labels.add( Long.decode( labelField.name() ) );
                    }
                }
            }

            return labels.iterator();
        }
        catch ( IOException e )
        {
            throw new RuntimeException( e );
        }
    }

    @Override
    public LabelScanWriter acquireWriter( final StorageService storage, Lock heldLock )
    {
        return new LuceneLabelScanWriter( storage, format, heldLock );
    }
}
