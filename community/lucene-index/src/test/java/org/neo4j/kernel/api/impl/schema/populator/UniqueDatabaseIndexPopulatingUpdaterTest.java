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
package org.neo4j.kernel.api.impl.schema.populator;

import org.junit.Test;

import java.util.Arrays;
import java.util.List;

import org.neo4j.collection.primitive.PrimitiveLongCollections;
import org.neo4j.kernel.api.impl.schema.SchemaIndex;
import org.neo4j.kernel.api.impl.schema.writer.LuceneIndexWriter;
import org.neo4j.kernel.api.index.PropertyAccessor;
import org.neo4j.kernel.impl.api.index.sampling.UniqueIndexSampler;
import org.neo4j.storageengine.api.schema.IndexSample;

import static org.hamcrest.Matchers.instanceOf;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyZeroInteractions;
import static org.neo4j.kernel.api.impl.schema.LuceneDocumentStructure.documentRepresentingProperty;
import static org.neo4j.kernel.api.impl.schema.LuceneDocumentStructure.newTermForChangeOrRemove;
import static org.neo4j.kernel.api.index.NodePropertyUpdate.add;
import static org.neo4j.kernel.api.index.NodePropertyUpdate.change;
import static org.neo4j.kernel.api.index.NodePropertyUpdate.remove;

public class UniqueDatabaseIndexPopulatingUpdaterTest
{
    private static final int PROPERTY_KEY = 42;

    @Test
    public void removeNotSupported()
    {
        UniqueLuceneIndexPopulatingUpdater updater = newUpdater();

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
    public void closeVerifiesUniquenessOfAddedValues() throws Exception
    {
        SchemaIndex index = mock( SchemaIndex.class );
        UniqueLuceneIndexPopulatingUpdater updater = newUpdater( index );

        updater.process( add( 1, PROPERTY_KEY, "foo", new long[]{1} ) );
        updater.process( add( 1, PROPERTY_KEY, "bar", new long[]{1} ) );
        updater.process( add( 1, PROPERTY_KEY, "baz", new long[]{1} ) );

        verifyZeroInteractions( index );

        updater.close();
        verify( index ).verifyUniqueness( any(), eq( PROPERTY_KEY ), eq( Arrays.asList( "foo", "bar", "baz" ) ) );
    }

    @Test
    public void closeVerifiesUniquenessOfChangedValues() throws Exception
    {
        SchemaIndex index = mock( SchemaIndex.class );
        UniqueLuceneIndexPopulatingUpdater updater = newUpdater( index );

        updater.process( change( 1, PROPERTY_KEY, "foo1", new long[]{1, 2}, "foo2", new long[]{1} ) );
        updater.process( change( 1, PROPERTY_KEY, "bar1", new long[]{1, 2}, "bar2", new long[]{1} ) );
        updater.process( change( 1, PROPERTY_KEY, "baz1", new long[]{1, 2}, "baz2", new long[]{1} ) );

        verifyZeroInteractions( index );

        updater.close();
        verify( index ).verifyUniqueness( any(), eq( PROPERTY_KEY ), eq( Arrays.asList( "foo2", "bar2", "baz2" ) ) );
    }

    @Test
    public void closeVerifiesUniquenessOfAddedAndChangedValues() throws Exception
    {
        SchemaIndex index = mock( SchemaIndex.class );
        UniqueLuceneIndexPopulatingUpdater updater = newUpdater( index );

        updater.process( add( 1, PROPERTY_KEY, "added1", new long[]{1} ) );
        updater.process( add( 2, PROPERTY_KEY, "added2", new long[]{1} ) );
        updater.process( change( 3, PROPERTY_KEY, "before1", new long[]{1, 2}, "after1", new long[]{2} ) );
        updater.process( change( 4, PROPERTY_KEY, "before2", new long[]{1}, "after2", new long[]{1, 2, 3} ) );
        updater.process( remove( 5, PROPERTY_KEY, "removed1", new long[]{1, 2} ) );

        verifyZeroInteractions( index );

        updater.close();

        List<Object> toBeVerified = Arrays.asList( "added1", "added2", "after1", "after2" );
        verify( index ).verifyUniqueness( any(), eq( PROPERTY_KEY ), eq( toBeVerified ) );
    }

    @Test
    public void addedNodePropertiesIncludedInSample() throws Exception
    {
        UniqueIndexSampler sampler = new UniqueIndexSampler();
        UniqueLuceneIndexPopulatingUpdater updater = newUpdater( sampler );

        updater.process( add( 1, PROPERTY_KEY, "foo", new long[]{1} ) );
        updater.process( add( 2, PROPERTY_KEY, "bar", new long[]{1} ) );
        updater.process( add( 3, PROPERTY_KEY, "baz", new long[]{1} ) );
        updater.process( add( 4, PROPERTY_KEY, "qux", new long[]{1} ) );

        verifySamplingResult( sampler, 4 );
    }

    @Test
    public void changedNodePropertiesDoNotInfluenceSample() throws Exception
    {
        UniqueIndexSampler sampler = new UniqueIndexSampler();
        UniqueLuceneIndexPopulatingUpdater updater = newUpdater( sampler );

        updater.process( change( 1, PROPERTY_KEY, "before1", new long[]{1}, "after1", new long[]{1} ) );
        updater.process( change( 2, PROPERTY_KEY, "before2", new long[]{1}, "after2", new long[]{1} ) );

        verifySamplingResult( sampler, 0 );
    }

    @Test
    public void removedNodePropertyIncludedInSample() throws Exception
    {
        long initialValue = 10;
        UniqueIndexSampler sampler = new UniqueIndexSampler();
        sampler.increment( initialValue );

        UniqueLuceneIndexPopulatingUpdater updater = newUpdater( sampler );

        updater.process( remove( 1, PROPERTY_KEY, "removed1", new long[]{1} ) );
        updater.process( remove( 2, PROPERTY_KEY, "removed2", new long[]{1} ) );

        verifySamplingResult( sampler, initialValue - 2 );
    }

    @Test
    public void nodePropertyUpdatesIncludedInSample() throws Exception
    {
        UniqueIndexSampler sampler = new UniqueIndexSampler();
        UniqueLuceneIndexPopulatingUpdater updater = newUpdater( sampler );

        updater.process( add( 1, PROPERTY_KEY, "foo", new long[]{1} ) );
        updater.process( change( 1, PROPERTY_KEY, "foo", new long[]{1}, "bar", new long[]{1} ) );
        updater.process( add( 2, PROPERTY_KEY, "baz", new long[]{1} ) );
        updater.process( add( 3, PROPERTY_KEY, "qux", new long[]{1} ) );
        updater.process( remove( 4, PROPERTY_KEY, "qux", new long[]{1} ) );

        verifySamplingResult( sampler, 2 );
    }

    @Test
    public void additionsDeliveredToIndexWriter() throws Exception
    {
        LuceneIndexWriter writer = mock( LuceneIndexWriter.class );
        UniqueLuceneIndexPopulatingUpdater updater = newUpdater( writer );

        updater.process( add( 1, PROPERTY_KEY, "foo", new long[]{1} ) );
        updater.process( add( 2, PROPERTY_KEY, "bar", new long[]{1} ) );
        updater.process( add( 3, PROPERTY_KEY, "qux", new long[]{1} ) );

        verify( writer ).updateDocument( newTermForChangeOrRemove( 1 ), documentRepresentingProperty( 1, "foo" ) );
        verify( writer ).updateDocument( newTermForChangeOrRemove( 2 ), documentRepresentingProperty( 2, "bar" ) );
        verify( writer ).updateDocument( newTermForChangeOrRemove( 3 ), documentRepresentingProperty( 3, "qux" ) );
    }

    @Test
    public void changesDeliveredToIndexWriter() throws Exception
    {
        LuceneIndexWriter writer = mock( LuceneIndexWriter.class );
        UniqueLuceneIndexPopulatingUpdater updater = newUpdater( writer );

        updater.process( change( 1, PROPERTY_KEY, "before1", new long[]{1}, "after1", new long[]{1} ) );
        updater.process( change( 2, PROPERTY_KEY, "before2", new long[]{1}, "after2", new long[]{1} ) );

        verify( writer ).updateDocument( newTermForChangeOrRemove( 1 ), documentRepresentingProperty( 1, "after1" ) );
        verify( writer ).updateDocument( newTermForChangeOrRemove( 2 ), documentRepresentingProperty( 2, "after2" ) );
    }

    @Test
    public void removalsDeliveredToIndexWriter() throws Exception
    {
        LuceneIndexWriter writer = mock( LuceneIndexWriter.class );
        UniqueLuceneIndexPopulatingUpdater updater = newUpdater( writer );

        updater.process( remove( 1, PROPERTY_KEY, "foo", new long[]{1} ) );
        updater.process( remove( 2, PROPERTY_KEY, "bar", new long[]{1} ) );
        updater.process( remove( 3, PROPERTY_KEY, "baz", new long[]{1} ) );

        verify( writer ).deleteDocuments( newTermForChangeOrRemove( 1 ) );
        verify( writer ).deleteDocuments( newTermForChangeOrRemove( 2 ) );
        verify( writer ).deleteDocuments( newTermForChangeOrRemove( 3 ) );
    }

    private static void verifySamplingResult( UniqueIndexSampler sampler, long expectedValue )
    {
        IndexSample sample = sampler.result();

        assertEquals( expectedValue, sample.indexSize() );
        assertEquals( expectedValue, sample.uniqueValues() );
        assertEquals( expectedValue, sample.sampleSize() );
    }

    private static UniqueLuceneIndexPopulatingUpdater newUpdater()
    {
        return newUpdater( new UniqueIndexSampler() );
    }

    private static UniqueLuceneIndexPopulatingUpdater newUpdater( SchemaIndex index )
    {
        return newUpdater( index, mock( LuceneIndexWriter.class ), new UniqueIndexSampler() );
    }

    private static UniqueLuceneIndexPopulatingUpdater newUpdater( LuceneIndexWriter writer )
    {
        return newUpdater( mock( SchemaIndex.class ), writer, new UniqueIndexSampler() );
    }

    private static UniqueLuceneIndexPopulatingUpdater newUpdater( UniqueIndexSampler sampler )
    {
        return newUpdater( mock( SchemaIndex.class ), mock( LuceneIndexWriter.class ), sampler );
    }

    private static UniqueLuceneIndexPopulatingUpdater newUpdater( SchemaIndex index, LuceneIndexWriter writer,
            UniqueIndexSampler sampler )
    {
        return new UniqueLuceneIndexPopulatingUpdater( writer, PROPERTY_KEY, index, mock( PropertyAccessor.class ),
                sampler );
    }
}
