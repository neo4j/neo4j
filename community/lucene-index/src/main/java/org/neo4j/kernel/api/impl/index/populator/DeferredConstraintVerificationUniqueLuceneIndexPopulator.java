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
package org.neo4j.kernel.api.impl.index.populator;

import org.apache.lucene.document.Document;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.neo4j.collection.primitive.PrimitiveLongSet;
import org.neo4j.kernel.api.exceptions.index.IndexEntryConflictException;
import org.neo4j.kernel.api.impl.index.LuceneIndex;
import org.neo4j.kernel.api.index.IndexDescriptor;
import org.neo4j.kernel.api.index.IndexUpdater;
import org.neo4j.kernel.api.index.NodePropertyUpdate;
import org.neo4j.kernel.api.index.PropertyAccessor;
import org.neo4j.kernel.impl.api.index.sampling.UniqueIndexSampler;
import org.neo4j.register.Register.DoubleLong;
import org.neo4j.storageengine.api.schema.IndexReader;

public class DeferredConstraintVerificationUniqueLuceneIndexPopulator extends LuceneIndexPopulator
{
    private final IndexDescriptor descriptor;
    private final UniqueIndexSampler sampler;

    public DeferredConstraintVerificationUniqueLuceneIndexPopulator(LuceneIndex index, IndexDescriptor descriptor )
    {
        super( index );
        this.descriptor = descriptor;
        this.sampler = new UniqueIndexSampler();
    }

    @Override
    public void create() throws IOException
    {
        super.create();
    }

    @Override
    protected void flush() throws IOException
    {
        // no need to do anything yet.
    }

    @Override
    public void add( long nodeId, Object propertyValue )
            throws IndexEntryConflictException, IOException
    {
        sampler.increment( 1 );
        Document doc = documentStructure.documentRepresentingProperty( nodeId, propertyValue );
        writer.addDocument( doc );
    }

    @Override
    public void verifyDeferredConstraints( PropertyAccessor accessor ) throws IndexEntryConflictException, IOException
    {
        luceneIndex.maybeRefresh();
        IndexReader indexReader;
        try
        {
            indexReader = luceneIndex.getIndexReader();
            indexReader.verifyDeferredConstraints(accessor, descriptor.getPropertyKeyId());
        }
        catch ( Exception e )
        {
            throw new IOException( e );
        }
        indexReader.close();
    }

    @Override
    public IndexUpdater newPopulatingUpdater( final PropertyAccessor accessor ) throws IOException
    {
        return new IndexUpdater()
        {
            List<Object> updatedPropertyValues = new ArrayList<>();

            @Override
            public void process( NodePropertyUpdate update ) throws IOException, IndexEntryConflictException
            {
                long nodeId = update.getNodeId();
                switch ( update.getUpdateMode() )
                {
                    case ADDED:
                        sampler.increment( 1 ); // add new value

                        // We don't look at the "before" value, so adding and changing idempotently is done the same way.
                        writer.updateDocument( documentStructure.newTermForChangeOrRemove( nodeId ),
                                documentStructure.documentRepresentingProperty( nodeId, update.getValueAfter() ) );
                        updatedPropertyValues.add( update.getValueAfter() );
                        break;
                    case CHANGED:
                        // do nothing on the sampler, since it would be something like:
                        // sampler.increment( -1 ); // remove old vale
                        // sampler.increment( 1 ); // add new value

                        // We don't look at the "before" value, so adding and changing idempotently is done the same way.
                        writer.updateDocument( documentStructure.newTermForChangeOrRemove( nodeId ),
                                documentStructure.documentRepresentingProperty( nodeId, update.getValueAfter() ) );
                        updatedPropertyValues.add( update.getValueAfter() );
                        break;
                    case REMOVED:
                        sampler.increment( -1 ); // remove old value
                        writer.deleteDocuments( documentStructure.newTermForChangeOrRemove( nodeId ) );
                        break;
                    default:
                        throw new IllegalStateException( "Unknown update mode " + update.getUpdateMode() );
                }
            }

            @Override
            public void close() throws IOException, IndexEntryConflictException
            {
                luceneIndex.maybeRefresh();
                try ( IndexReader indexReader = luceneIndex.getIndexReader() )
                {
                    indexReader.verifyDeferredConstraints( accessor, descriptor.getPropertyKeyId(),
                            updatedPropertyValues );
                }
                catch ( Exception e )
                {
                    throw new IOException( e );
                }
            }

            @Override
            public void remove( PrimitiveLongSet nodeIds )
            {
                throw new UnsupportedOperationException( "should not remove() from populating index" );
            }
        };
    }

    @Override
    public long sampleResult( DoubleLong.Out result )
    {
        return sampler.result( result );
    }
}
