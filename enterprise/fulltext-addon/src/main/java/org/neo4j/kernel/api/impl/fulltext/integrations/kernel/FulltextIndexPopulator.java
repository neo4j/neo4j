/*
 * Copyright (c) 2002-2018 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.kernel.api.impl.fulltext.integrations.kernel;

import org.apache.lucene.document.Document;

import java.io.IOException;
import java.util.Collection;

import org.neo4j.io.IOUtils;
import org.neo4j.kernel.api.exceptions.index.IndexEntryConflictException;
import org.neo4j.kernel.api.impl.fulltext.lucene.LuceneFulltextDocumentStructure;
import org.neo4j.kernel.api.impl.fulltext.lucene.WritableFulltext;
import org.neo4j.kernel.api.index.IndexEntryUpdate;
import org.neo4j.kernel.api.index.IndexPopulator;
import org.neo4j.kernel.api.index.IndexUpdater;
import org.neo4j.kernel.api.index.PropertyAccessor;
import org.neo4j.kernel.impl.api.index.sampling.IndexSamplingConfig;
import org.neo4j.storageengine.api.schema.IndexSample;

public class FulltextIndexPopulator implements IndexPopulator
{
    private final FulltextIndexDescriptor descriptor;
    private final WritableFulltext index;

    FulltextIndexPopulator( long indexId, FulltextIndexDescriptor descriptor, IndexSamplingConfig samplingConfig, WritableFulltext index )
    {
        this.descriptor = descriptor;
        this.index = index;
    }

    @Override
    public void create() throws IOException
    {
        index.create();
        index.open();
    }

    @Override
    public void drop() throws IOException
    {
        index.drop();
    }

    @Override
    public void add( Collection<? extends IndexEntryUpdate<?>> updates ) throws IndexEntryConflictException, IOException
    {
        index.getIndexWriter().addDocuments( updates.size(), () -> updates.stream().map( this::updateAsDocument ).iterator() );
    }

    @Override
    public void verifyDeferredConstraints( PropertyAccessor propertyAccessor ) throws IndexEntryConflictException, IOException
    {
        //Sure whatever
    }

    @Override
    public IndexUpdater newPopulatingUpdater( PropertyAccessor accessor ) throws IOException
    {
        return new PopulatingFulltextIndexUpdater();
    }

    @Override
    public void close( boolean populationCompletedSuccessfully ) throws IOException
    {
        try
        {
            if ( populationCompletedSuccessfully )
            {
                index.markAsOnline();
            }
        }
        finally
        {
            IOUtils.closeAllSilently( index );
        }
    }

    @Override
    public void markAsFailed( String failure ) throws IOException
    {
        index.markAsFailed( failure );
    }

    @Override
    public void includeSample( IndexEntryUpdate<?> update )
    {
        //Index sampling is not our thing, really.
    }

    @Override
    public IndexSample sampleResult()
    {
        return new IndexSample();
    }

    private Document updateAsDocument( IndexEntryUpdate<?> update )
    {
        return LuceneFulltextDocumentStructure.documentRepresentingProperties( update.getEntityId(), descriptor.propertyNames(), update.values() );
    }

    private class PopulatingFulltextIndexUpdater implements IndexUpdater
    {

        @Override
        public void process( IndexEntryUpdate<?> update ) throws IOException
        {
            //TODO is this comment copy pasted? What does it mean?
            // we do not support adding partial entries
            assert update.indexKey().schema().equals( descriptor.schema() );

            switch ( update.updateMode() )
            {
            case ADDED:

                long nodeId = update.getEntityId();
                index.getIndexWriter().updateDocument( LuceneFulltextDocumentStructure.newTermForChangeOrRemove( nodeId ),
                        LuceneFulltextDocumentStructure.documentRepresentingProperties( nodeId, descriptor.propertyNames(), update.values() ) );

            case CHANGED:
                long nodeId1 = update.getEntityId();
                index.getIndexWriter().updateDocument( LuceneFulltextDocumentStructure.newTermForChangeOrRemove( nodeId1 ),
                        LuceneFulltextDocumentStructure.documentRepresentingProperties( nodeId1, descriptor.propertyNames(), update.values() ) );
                break;
            case REMOVED:
                index.getIndexWriter().deleteDocuments( LuceneFulltextDocumentStructure.newTermForChangeOrRemove( update.getEntityId() ) );
                break;
            default:
                throw new UnsupportedOperationException();
            }
        }

        @Override
        public void close()
        {
        }
    }
}
