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
package org.neo4j.kernel.api.impl.index;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.neo4j.collection.primitive.PrimitiveLongSet;
import org.neo4j.kernel.api.exceptions.index.IndexEntryConflictException;
import org.neo4j.kernel.api.index.IndexUpdater;
import org.neo4j.kernel.api.index.PropertyAccessor;
import org.neo4j.kernel.api.index.util.FailureStorage;
import org.neo4j.kernel.impl.api.index.sampling.IndexSamplingConfig;
import org.neo4j.kernel.impl.api.index.sampling.NonUniqueIndexSampler;
import org.neo4j.kernel.impl.store.index.NodePropertyUpdate;
import org.neo4j.register.Register;

class NonUniqueLuceneIndexPopulator extends LuceneIndexPopulator
{
    static final int DEFAULT_QUEUE_THRESHOLD = 10000;
    private final int queueThreshold;
    private final NonUniqueIndexSampler sampler;
    private final List<NodePropertyUpdate> updates = new ArrayList<>();

    NonUniqueLuceneIndexPopulator( int queueThreshold, LuceneDocumentStructure documentStructure,
                                   IndexWriterFactory<LuceneIndexWriter> indexWriterFactory,
                                   DirectoryFactory dirFactory, File dirFile, FailureStorage failureStorage,
                                   long indexId, IndexSamplingConfig samplingConfig )
    {
        super( documentStructure, indexWriterFactory, dirFactory, dirFile, failureStorage, indexId );
        this.queueThreshold = queueThreshold;
        this.sampler = new NonUniqueIndexSampler( samplingConfig.bufferSize() );
    }

    @Override
    public void add( long nodeId, Object propertyValue ) throws IOException
    {
        sampler.include( documentStructure.encodedStringValue( propertyValue ) );
        writer.addDocument( documentStructure.documentRepresentingProperty( nodeId, propertyValue ) );
    }

    @Override
    public void verifyDeferredConstraints( PropertyAccessor accessor ) throws IndexEntryConflictException, IOException
    {
        // no constraints to verify so do nothing
    }

    @Override
    public IndexUpdater newPopulatingUpdater( PropertyAccessor propertyAccessor ) throws IOException
    {
        return new IndexUpdater()
        {
            @Override
            public void process( NodePropertyUpdate update ) throws IOException, IndexEntryConflictException
            {
                switch ( update.getUpdateMode() )
                {
                    case ADDED:
                        // We don't look at the "before" value, so adding and changing idempotently is done the same way.
                        String encodedValue = documentStructure.encodedStringValue( update.getValueAfter() );
                        sampler.include( encodedValue );
                        break;
                    case CHANGED:
                        // We don't look at the "before" value, so adding and changing idempotently is done the same way.
                        String encodedValueBefore = documentStructure.encodedStringValue( update.getValueBefore() );
                        sampler.exclude( encodedValueBefore );
                        String encodedValueAfter = documentStructure.encodedStringValue( update.getValueAfter() );
                        sampler.include( encodedValueAfter );
                        break;
                    case REMOVED:
                        String removedValue = documentStructure.encodedStringValue( update.getValueBefore() );
                        sampler.exclude( removedValue );
                        break;
                    default:
                        throw new IllegalStateException( "Unknown update mode " + update.getUpdateMode() );
                }

                updates.add( update );
            }

            @Override
            public void close() throws IOException, IndexEntryConflictException
            {
                if ( updates.size() > queueThreshold )
                {
                    flush();
                    updates.clear();
                }

            }

            @Override
            public void remove( PrimitiveLongSet nodeIds ) throws IOException
            {
                throw new UnsupportedOperationException( "Should not remove() from populating index." );
            }
        };
    }

    @Override
    public long sampleResult( Register.DoubleLong.Out result )
    {
        return sampler.result( result );
    }

    @Override
    protected void flush() throws IOException
    {
        for ( NodePropertyUpdate update : this.updates )
        {
            long nodeId = update.getNodeId();
            switch ( update.getUpdateMode() )
            {
            case ADDED:
            case CHANGED:
                // We don't look at the "before" value, so adding and changing idempotently is done the same way.
                writer.updateDocument( documentStructure.newTermForChangeOrRemove( nodeId ),
                                       documentStructure.documentRepresentingProperty( nodeId, update.getValueAfter() ) );
                break;
            case REMOVED:
                writer.deleteDocuments( documentStructure.newTermForChangeOrRemove( nodeId ) );
                break;
            default:
                throw new IllegalStateException( "Unknown update mode " + update.getUpdateMode() );
            }
        }
    }
}
