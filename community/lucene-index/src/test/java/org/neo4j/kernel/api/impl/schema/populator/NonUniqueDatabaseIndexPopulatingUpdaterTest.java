/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
package org.neo4j.kernel.api.impl.schema.populator;

import org.junit.Test;

import org.neo4j.collection.primitive.PrimitiveLongCollections;
import org.neo4j.kernel.api.impl.schema.writer.LuceneIndexWriter;
import org.neo4j.kernel.api.schema_new.index.NewIndexDescriptor;
import org.neo4j.kernel.api.schema_new.index.NewIndexDescriptorFactory;
import org.neo4j.kernel.impl.api.index.sampling.DefaultNonUniqueIndexSampler;
import org.neo4j.kernel.impl.api.index.sampling.NonUniqueIndexSampler;
import org.neo4j.storageengine.api.schema.IndexSample;

import static org.hamcrest.Matchers.instanceOf;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.neo4j.kernel.api.impl.schema.LuceneDocumentStructure.documentRepresentingProperty;
import static org.neo4j.kernel.api.impl.schema.LuceneDocumentStructure.newTermForChangeOrRemove;
import static org.neo4j.kernel.api.index.IndexEntryUpdate.add;
import static org.neo4j.kernel.api.index.IndexEntryUpdate.change;
import static org.neo4j.kernel.api.index.IndexEntryUpdate.remove;

public class NonUniqueDatabaseIndexPopulatingUpdaterTest
{
    private static final int PROPERTY_KEY = 42;
    private static final int SAMPLING_BUFFER_SIZE_LIMIT = 100;
    private static final NewIndexDescriptor index = NewIndexDescriptorFactory.forLabel( 1, PROPERTY_KEY );

    @Test
    public void removeNotSupported()
    {
        NonUniqueLuceneIndexPopulatingUpdater updater = newUpdater();

        try
        {
            updater.remove( PrimitiveLongCollections.setOf( 1, 2, 3 ) );
            fail( "Exception expected" );
        }
        catch ( Exception e )
        {
            assertThat( e, instanceOf( UnsupportedOperationException.class ) );
        }
    }

    @Test
    public void addedNodePropertiesIncludedInSample() throws Exception
    {
        NonUniqueIndexSampler sampler = newSampler();
        NonUniqueLuceneIndexPopulatingUpdater updater = newUpdater( sampler );

        updater.process( add( 1, index, "foo" ) );
        updater.process( add( 2, index, "bar" ) );
        updater.process( add( 3, index, "baz" ) );
        updater.process( add( 4, index, "bar" ) );

        verifySamplingResult( sampler, 4, 3, 4 );
    }

    @Test
    public void changedNodePropertiesIncludedInSample() throws Exception
    {
        NonUniqueIndexSampler sampler = newSampler();
        NonUniqueLuceneIndexPopulatingUpdater updater = newUpdater( sampler );

        updater.process( add( 1, index, "initial1" ) );
        updater.process( add( 2, index, "initial2" ) );
        updater.process( add( 3, index, "new2" ) );

        updater.process( change( 1, index, "initial1", "new1" ) );
        updater.process( change( 1, index, "initial2", "new2" ) );

        verifySamplingResult( sampler, 3, 2, 3 );
    }

    @Test
    public void removedNodePropertyIncludedInSample() throws Exception
    {
        NonUniqueIndexSampler sampler = newSampler();
        NonUniqueLuceneIndexPopulatingUpdater updater = newUpdater( sampler );

        updater.process( add( 1, index, "foo" ) );
        updater.process( add( 2, index, "bar" ) );
        updater.process( add( 3, index, "baz" ) );
        updater.process( add( 4, index, "qux" ) );

        updater.process( remove( 1, index, "foo" ) );
        updater.process( remove( 2, index, "bar" ) );
        updater.process( remove( 4, index, "qux" ) );

        verifySamplingResult( sampler, 1, 1, 1 );
    }

    @Test
    public void nodePropertyUpdatesIncludedInSample() throws Exception
    {
        NonUniqueIndexSampler sampler = newSampler();
        NonUniqueLuceneIndexPopulatingUpdater updater = newUpdater( sampler );

        updater.process( add( 1, index, "foo" ) );
        updater.process( change( 1, index, "foo", "newFoo1" ) );

        updater.process( add( 2, index, "bar" ) );
        updater.process( remove( 2, index, "bar" ) );

        updater.process( change( 1, index, "newFoo1", "newFoo2" ) );

        updater.process( add( 42, index, "qux" ) );
        updater.process( add( 3, index, "bar" ) );
        updater.process( add( 4, index, "baz" ) );
        updater.process( add( 5, index, "bar" ) );
        updater.process( remove( 42, index, "qux" ) );

        verifySamplingResult( sampler, 4, 3, 4 );
    }

    @Test
    public void additionsDeliveredToIndexWriter() throws Exception
    {
        LuceneIndexWriter writer = mock( LuceneIndexWriter.class );
        NonUniqueLuceneIndexPopulatingUpdater updater = newUpdater( writer );

        updater.process( add( 1, index, "foo" ) );
        updater.process( add( 2, index, "bar" ) );
        updater.process( add( 3, index, "qux" ) );

        verify( writer ).updateDocument( newTermForChangeOrRemove( 1 ), documentRepresentingProperty( 1, "foo" ) );
        verify( writer ).updateDocument( newTermForChangeOrRemove( 2 ), documentRepresentingProperty( 2, "bar" ) );
        verify( writer ).updateDocument( newTermForChangeOrRemove( 3 ), documentRepresentingProperty( 3, "qux" ) );
    }

    @Test
    public void changesDeliveredToIndexWriter() throws Exception
    {
        LuceneIndexWriter writer = mock( LuceneIndexWriter.class );
        NonUniqueLuceneIndexPopulatingUpdater updater = newUpdater( writer );

        updater.process( change( 1, index, "before1", "after1" ) );
        updater.process( change( 2, index, "before2", "after2" ) );

        verify( writer ).updateDocument( newTermForChangeOrRemove( 1 ), documentRepresentingProperty( 1, "after1" ) );
        verify( writer ).updateDocument( newTermForChangeOrRemove( 2 ), documentRepresentingProperty( 2, "after2" ) );
    }

    @Test
    public void removalsDeliveredToIndexWriter() throws Exception
    {
        LuceneIndexWriter writer = mock( LuceneIndexWriter.class );
        NonUniqueLuceneIndexPopulatingUpdater updater = newUpdater( writer );

        updater.process( remove( 1, index, "foo" ) );
        updater.process( remove( 2, index, "bar" ) );
        updater.process( remove( 3, index, "baz" ) );

        verify( writer ).deleteDocuments( newTermForChangeOrRemove( 1 ) );
        verify( writer ).deleteDocuments( newTermForChangeOrRemove( 2 ) );
        verify( writer ).deleteDocuments( newTermForChangeOrRemove( 3 ) );
    }

    private static void verifySamplingResult( NonUniqueIndexSampler sampler, long expectedIndexSize,
            long expectedUniqueValues, long expectedSampleSize )
    {
        IndexSample sample = sampler.result();

        assertEquals( expectedIndexSize, sample.indexSize() );
        assertEquals( expectedUniqueValues, sample.uniqueValues() );
        assertEquals( expectedSampleSize, sample.sampleSize() );
    }

    private static NonUniqueLuceneIndexPopulatingUpdater newUpdater()
    {
        return newUpdater( newSampler() );
    }

    private static NonUniqueLuceneIndexPopulatingUpdater newUpdater( NonUniqueIndexSampler sampler )
    {
        return newUpdater( mock( LuceneIndexWriter.class ), sampler );
    }

    private static NonUniqueLuceneIndexPopulatingUpdater newUpdater( LuceneIndexWriter writer )
    {
        return newUpdater( writer, newSampler() );
    }

    private static NonUniqueLuceneIndexPopulatingUpdater newUpdater( LuceneIndexWriter writer,
            NonUniqueIndexSampler sampler )
    {
        return new NonUniqueLuceneIndexPopulatingUpdater( writer, sampler );
    }

    private static NonUniqueIndexSampler newSampler()
    {
        return new DefaultNonUniqueIndexSampler( SAMPLING_BUFFER_SIZE_LIMIT );
    }
}
