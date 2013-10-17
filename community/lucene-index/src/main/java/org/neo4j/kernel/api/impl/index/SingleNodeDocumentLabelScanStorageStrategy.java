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
import java.util.Iterator;

import org.apache.lucene.document.Document;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.IndexSearcher;

import org.neo4j.index.impl.lucene.Hits;
import org.neo4j.kernel.api.scan.NodeLabelUpdate;
import org.neo4j.kernel.impl.api.PrimitiveLongIterator;

/**
 * {@link org.neo4j.kernel.api.scan.LabelScanStore} implemented using Lucene. There's only one big index for all labels
 * because
 * the Lucene document structure handles that quite efficiently. It's as follows (pseudo field keys):
 *
 * { // document for node 1
 * id: 1
 * label: 4
 * label: 2
 * label: 56
 * }
 * { // document for node 2
 * id: 2
 * label: 4
 * }
 */
public class SingleNodeDocumentLabelScanStorageStrategy implements LabelScanStorageStrategy
{
    private static final String LABEL_FIELD_IDENTIFIER = "label";
    private final LuceneDocumentStructure documentStructure;

    public SingleNodeDocumentLabelScanStorageStrategy( LuceneDocumentStructure luceneDocumentStructure )
    {
        this.documentStructure = luceneDocumentStructure;
    }

    @Override
    public String toString()
    {
        return getClass().getSimpleName();
    }

    @Override
    public void applyUpdates( StorageService storage, Iterator<NodeLabelUpdate> updates ) throws IOException
    {
        while ( updates.hasNext() )
        {
            NodeLabelUpdate update = updates.next();
            Term documentTerm = documentStructure.newQueryForChangeOrRemove( update.getNodeId() );
            if ( update.getLabelsAfter().length > 0 )
            {
                // Delete any existing document for this node and index the current set of labels
                Document document = documentStructure.newDocument( update.getNodeId() );
                for ( long label : update.getLabelsAfter() )
                {
                    document.add( documentStructure.newField( LABEL_FIELD_IDENTIFIER, label ) );
                }
                storage.updateDocument( documentTerm, document );
            }
            else
            {
                // Delete the document for this node from the index
                storage.deleteDocuments( documentTerm );
            }
        }
    }

    @Override
    public PrimitiveLongIterator nodesWithLabel( IndexSearcher searcher, int labelId )
    {
        try
        {
            Hits hits = new Hits( searcher, documentStructure.newQuery( LABEL_FIELD_IDENTIFIER, labelId ), null );
            return new HitsPrimitiveLongIterator( hits, documentStructure );
        }
        catch ( IOException e )
        {
            throw new RuntimeException( e );
        }
    }
}
