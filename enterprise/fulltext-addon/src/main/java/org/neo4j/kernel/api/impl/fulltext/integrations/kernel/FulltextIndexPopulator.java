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
import java.util.HashMap;
import java.util.Map;

import org.neo4j.kernel.api.exceptions.index.IndexEntryConflictException;
import org.neo4j.kernel.api.impl.fulltext.lucene.LuceneFulltextDocumentStructure;
import org.neo4j.kernel.api.impl.fulltext.lucene.WritableFulltext;
import org.neo4j.kernel.api.index.IndexEntryUpdate;
import org.neo4j.kernel.api.index.IndexPopulator;
import org.neo4j.kernel.api.index.IndexUpdater;
import org.neo4j.kernel.api.index.PropertyAccessor;
import org.neo4j.kernel.impl.api.index.sampling.IndexSamplingConfig;
import org.neo4j.storageengine.api.schema.IndexSample;
import org.neo4j.values.storable.Value;

public class FulltextIndexPopulator implements IndexPopulator
{
    private final long indexId;
    private final FulltextIndexDescriptor descriptor;
    private final IndexSamplingConfig samplingConfig;
    private final WritableFulltext index;

    public FulltextIndexPopulator( long indexId, FulltextIndexDescriptor descriptor, IndexSamplingConfig samplingConfig, WritableFulltext index )
    {
        this.indexId = indexId;
        this.descriptor = descriptor;
        this.samplingConfig = samplingConfig;
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
        System.out.println( "Got updates!!" + updates.size() );
        index.getIndexWriter().addDocuments( updates.size(), () -> updates.stream().map( this::updateAsDocument ).iterator() );
        //Ingore for now
//        throw new UnsupportedOperationException( "not implemented" );
//        for ( IndexEntryUpdate<?> update : updates )
//        {
//            update.updateMode()
//        }

    }

    @Override
    public void verifyDeferredConstraints( PropertyAccessor propertyAccessor ) throws IndexEntryConflictException, IOException
    {
        //Sure whatever
    }

    @Override
    public IndexUpdater newPopulatingUpdater( PropertyAccessor accessor ) throws IOException
    {
        System.out.println( "tried to get populating updater" );
        throw new UnsupportedOperationException( "not implemented" );
    }

    @Override
    public void close( boolean populationCompletedSuccessfully ) throws IOException
    {
        //TODO whatever
        if ( populationCompletedSuccessfully )
        {
            index.markAsOnline();
        }
        index.close();

        System.out.println("close population");

    }

    @Override
    public void markAsFailed( String failure ) throws IOException
    {
        //TODO whatever
        System.out.println( "Mark as failed" );
        System.out.println( failure );

    }

    @Override
    public void includeSample( IndexEntryUpdate<?> update )
    {
        //Sure, I guess?
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
}
