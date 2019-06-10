/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
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
package org.neo4j.kernel.api.impl.fulltext;

import org.apache.lucene.document.Document;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import org.neo4j.function.ThrowingAction;
import org.neo4j.kernel.api.impl.index.DatabaseIndex;
import org.neo4j.kernel.api.impl.schema.populator.LuceneIndexPopulator;
import org.neo4j.kernel.api.index.IndexEntryUpdate;
import org.neo4j.kernel.api.index.IndexUpdater;
import org.neo4j.storageengine.api.NodePropertyAccessor;
import org.neo4j.storageengine.api.schema.IndexSample;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.Values;

public class FulltextIndexPopulator extends LuceneIndexPopulator<DatabaseIndex<FulltextIndexReader>>
{
    private final FulltextIndexDescriptor descriptor;
    private final ThrowingAction<IOException> descriptorCreateAction;

    public FulltextIndexPopulator( FulltextIndexDescriptor descriptor, DatabaseIndex<FulltextIndexReader> luceneFulltext,
                                  ThrowingAction<IOException> descriptorCreateAction )
    {
        super( luceneFulltext );
        this.descriptor = descriptor;
        this.descriptorCreateAction = descriptorCreateAction;
    }

    @Override
    public void create()
    {
        super.create();
        try
        {
            descriptorCreateAction.apply();
        }
        catch ( IOException e )
        {
            throw new UncheckedIOException( e );
        }
    }

    @Override
    public void add( Collection<? extends IndexEntryUpdate<?>> updates )
    {
        try
        {
            for ( IndexEntryUpdate<?> update : updates )
            {
                writer.updateDocument( LuceneFulltextDocumentStructure.newTermForChangeOrRemove( update.getEntityId() ), updateAsDocument( update ) );
            }
        }
        catch ( IOException e )
        {
            throw new UncheckedIOException( e );
        }
    }

    @Override
    public void verifyDeferredConstraints( NodePropertyAccessor propertyAccessor )
    {
        //Fulltext index does not care about constraints.
    }

    @Override
    public IndexUpdater newPopulatingUpdater( NodePropertyAccessor accessor )
    {
        return new PopulatingFulltextIndexUpdater();
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

    @Override
    public Map<String,Value> indexConfig()
    {
        Map<String,Value> map = new HashMap<>();
        map.put( FulltextIndexSettings.INDEX_CONFIG_ANALYZER, Values.stringValue( descriptor.analyzerName() ) );
        map.put( FulltextIndexSettings.INDEX_CONFIG_EVENTUALLY_CONSISTENT, Values.booleanValue( descriptor.isEventuallyConsistent() ) );
        return map;
    }

    private Document updateAsDocument( IndexEntryUpdate<?> update )
    {
        return LuceneFulltextDocumentStructure.documentRepresentingProperties( update.getEntityId(), descriptor.propertyNames(), update.values() );
    }

    private class PopulatingFulltextIndexUpdater implements IndexUpdater
    {
        @Override
        public void process( IndexEntryUpdate<?> update )
        {
            assert update.indexKey().schema().equals( descriptor.schema() );
            try
            {
                switch ( update.updateMode() )
                {
                case ADDED:
                    long nodeId = update.getEntityId();
                    luceneIndex.getIndexWriter().updateDocument( LuceneFulltextDocumentStructure.newTermForChangeOrRemove( nodeId ),
                            LuceneFulltextDocumentStructure.documentRepresentingProperties( nodeId, descriptor.propertyNames(), update.values() ) );

                case CHANGED:
                    long nodeId1 = update.getEntityId();
                    luceneIndex.getIndexWriter().updateDocument( LuceneFulltextDocumentStructure.newTermForChangeOrRemove( nodeId1 ),
                            LuceneFulltextDocumentStructure.documentRepresentingProperties( nodeId1, descriptor.propertyNames(), update.values() ) );
                    break;
                case REMOVED:
                    luceneIndex.getIndexWriter().deleteDocuments( LuceneFulltextDocumentStructure.newTermForChangeOrRemove( update.getEntityId() ) );
                    break;
                default:
                    throw new UnsupportedOperationException();
                }
            }
            catch ( IOException e )
            {
                throw new UncheckedIOException( e );
            }
        }

        @Override
        public void close()
        {
        }
    }
}
