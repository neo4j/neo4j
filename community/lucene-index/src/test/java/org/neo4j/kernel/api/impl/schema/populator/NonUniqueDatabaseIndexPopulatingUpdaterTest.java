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
package org.neo4j.kernel.api.impl.schema.populator;

import org.apache.lucene.index.Term;
import org.junit.Test;

import java.io.IOException;

import org.neo4j.internal.kernel.api.schema.SchemaDescriptor;
import org.neo4j.kernel.api.impl.schema.writer.LuceneIndexWriter;
import org.neo4j.kernel.api.schema.SchemaDescriptorFactory;
import org.neo4j.kernel.impl.api.index.sampling.DefaultNonUniqueIndexSampler;
import org.neo4j.kernel.impl.api.index.sampling.NonUniqueIndexSampler;
import org.neo4j.storageengine.api.schema.IndexSample;

import static org.hamcrest.Matchers.hasToString;
import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.hamcrest.MockitoHamcrest.argThat;
import static org.neo4j.kernel.api.impl.LuceneTestUtil.documentRepresentingProperties;
import static org.neo4j.kernel.api.impl.schema.LuceneDocumentStructure.newTermForChangeOrRemove;
import static org.neo4j.kernel.api.index.IndexQueryHelper.add;
import static org.neo4j.kernel.api.index.IndexQueryHelper.change;
import static org.neo4j.kernel.api.index.IndexQueryHelper.remove;

public class NonUniqueDatabaseIndexPopulatingUpdaterTest
{
    private static final SchemaDescriptor SCHEMA_DESCRIPTOR = SchemaDescriptorFactory.forLabel( 1, 42 );
    private static final int SAMPLING_BUFFER_SIZE_LIMIT = 100;
    private static final SchemaDescriptor COMPOSITE_SCHEMA_DESCRIPTOR = SchemaDescriptorFactory
            .forLabel( 1, 42, 43 );

    @Test
    public void addedNodePropertiesIncludedInSample() throws Exception
    {
        NonUniqueIndexSampler sampler = newSampler();
        NonUniqueLuceneIndexPopulatingUpdater updater = newUpdater( sampler );

        updater.process( add( 1, SCHEMA_DESCRIPTOR, "foo" ) );
        updater.process( add( 2, SCHEMA_DESCRIPTOR, "bar" ) );
        updater.process( add( 3, SCHEMA_DESCRIPTOR, "baz" ) );
        updater.process( add( 4, SCHEMA_DESCRIPTOR, "bar" ) );

        verifySamplingResult( sampler, 4, 3, 4 );
    }

    @Test
    public void addedNodeCompositePropertiesIncludedInSample() throws Exception
    {
        NonUniqueIndexSampler sampler = newSampler();
        NonUniqueLuceneIndexPopulatingUpdater updater = newUpdater( sampler );
        updater.process( add( 1, COMPOSITE_SCHEMA_DESCRIPTOR, "bit", "foo" ) );
        updater.process( add( 2, COMPOSITE_SCHEMA_DESCRIPTOR, "bit", "bar" ) );
        updater.process( add( 3, COMPOSITE_SCHEMA_DESCRIPTOR, "bit", "baz" ) );
        updater.process( add( 4, COMPOSITE_SCHEMA_DESCRIPTOR, "bit", "bar" ) );

        verifySamplingResult( sampler, 4, 3, 4 );
    }

    @Test
    public void changedNodePropertiesIncludedInSample() throws Exception
    {
        NonUniqueIndexSampler sampler = newSampler();
        NonUniqueLuceneIndexPopulatingUpdater updater = newUpdater( sampler );

        updater.process( add( 1, SCHEMA_DESCRIPTOR, "initial1" ) );
        updater.process( add( 2, SCHEMA_DESCRIPTOR, "initial2" ) );
        updater.process( add( 3, SCHEMA_DESCRIPTOR, "new2" ) );

        updater.process( change( 1, SCHEMA_DESCRIPTOR, "initial1", "new1" ) );
        updater.process( change( 1, SCHEMA_DESCRIPTOR, "initial2", "new2" ) );

        verifySamplingResult( sampler, 3, 2, 3 );
    }

    @Test
    public void changedNodeCompositePropertiesIncludedInSample() throws Exception
    {
        NonUniqueIndexSampler sampler = newSampler();
        NonUniqueLuceneIndexPopulatingUpdater updater = newUpdater( sampler );

        updater.process( add( 1, COMPOSITE_SCHEMA_DESCRIPTOR, "bit", "initial1" ) );
        updater.process( add( 2, COMPOSITE_SCHEMA_DESCRIPTOR, "bit", "initial2" ) );
        updater.process( add( 3, COMPOSITE_SCHEMA_DESCRIPTOR, "bit", "new2" ) );

        updater.process( change( 1,
                COMPOSITE_SCHEMA_DESCRIPTOR, new Object[]{"bit", "initial1"}, new Object[]{"bit", "new1"} ) );
        updater.process( change( 1,
                COMPOSITE_SCHEMA_DESCRIPTOR, new Object[]{"bit", "initial2"}, new Object[]{"bit", "new2"} ) );

        verifySamplingResult( sampler, 3, 2, 3 );
    }

    @Test
    public void removedNodePropertyIncludedInSample() throws Exception
    {
        NonUniqueIndexSampler sampler = newSampler();
        NonUniqueLuceneIndexPopulatingUpdater updater = newUpdater( sampler );

        updater.process( add( 1, SCHEMA_DESCRIPTOR, "foo" ) );
        updater.process( add( 2, SCHEMA_DESCRIPTOR, "bar" ) );
        updater.process( add( 3, SCHEMA_DESCRIPTOR, "baz" ) );
        updater.process( add( 4, SCHEMA_DESCRIPTOR, "qux" ) );

        updater.process( remove( 1, SCHEMA_DESCRIPTOR, "foo" ) );
        updater.process( remove( 2, SCHEMA_DESCRIPTOR, "bar" ) );
        updater.process( remove( 4, SCHEMA_DESCRIPTOR, "qux" ) );

        verifySamplingResult( sampler, 1, 1, 1 );
    }

    @Test
    public void removedNodeCompositePropertyIncludedInSample() throws Exception
    {
        NonUniqueIndexSampler sampler = newSampler();
        NonUniqueLuceneIndexPopulatingUpdater updater = newUpdater( sampler );

        updater.process( add( 1, COMPOSITE_SCHEMA_DESCRIPTOR, "bit", "foo" ) );
        updater.process( add( 2, COMPOSITE_SCHEMA_DESCRIPTOR, "bit", "bar" ) );
        updater.process( add( 3, COMPOSITE_SCHEMA_DESCRIPTOR, "bit", "baz" ) );
        updater.process( add( 4, COMPOSITE_SCHEMA_DESCRIPTOR, "bit", "qux" ) );

        updater.process( remove( 1, COMPOSITE_SCHEMA_DESCRIPTOR, "bit", "foo" ) );
        updater.process( remove( 2, COMPOSITE_SCHEMA_DESCRIPTOR, "bit", "bar" ) );
        updater.process( remove( 4, COMPOSITE_SCHEMA_DESCRIPTOR, "bit", "qux" ) );

        verifySamplingResult( sampler, 1, 1, 1 );
    }

    @Test
    public void nodePropertyUpdatesIncludedInSample() throws Exception
    {
        NonUniqueIndexSampler sampler = newSampler();
        NonUniqueLuceneIndexPopulatingUpdater updater = newUpdater( sampler );

        updater.process( add( 1, SCHEMA_DESCRIPTOR, "foo" ) );
        updater.process( change( 1, SCHEMA_DESCRIPTOR, "foo", "newFoo1" ) );

        updater.process( add( 2, SCHEMA_DESCRIPTOR, "bar" ) );
        updater.process( remove( 2, SCHEMA_DESCRIPTOR, "bar" ) );

        updater.process( change( 1, SCHEMA_DESCRIPTOR, "newFoo1", "newFoo2" ) );

        updater.process( add( 42, SCHEMA_DESCRIPTOR, "qux" ) );
        updater.process( add( 3, SCHEMA_DESCRIPTOR, "bar" ) );
        updater.process( add( 4, SCHEMA_DESCRIPTOR, "baz" ) );
        updater.process( add( 5, SCHEMA_DESCRIPTOR, "bar" ) );
        updater.process( remove( 42, SCHEMA_DESCRIPTOR, "qux" ) );

        verifySamplingResult( sampler, 4, 3, 4 );
    }

    @Test
    public void nodeCompositePropertyUpdatesIncludedInSample() throws Exception
    {
        NonUniqueIndexSampler sampler = newSampler();
        NonUniqueLuceneIndexPopulatingUpdater updater = newUpdater( sampler );

        updater.process( add( 1, COMPOSITE_SCHEMA_DESCRIPTOR, "bit", "foo" ) );
        updater.process( change( 1,
                COMPOSITE_SCHEMA_DESCRIPTOR, new Object[]{"bit", "foo"}, new Object[]{"bit", "newFoo1"} ) );

        updater.process( add( 2, COMPOSITE_SCHEMA_DESCRIPTOR, "bit", "bar" ) );
        updater.process( remove( 2, COMPOSITE_SCHEMA_DESCRIPTOR, "bit", "bar" ) );

        updater.process( change( 1,
                COMPOSITE_SCHEMA_DESCRIPTOR, new Object[]{"bit", "newFoo1"}, new Object[]{"bit", "newFoo2"} ) );

        updater.process( add( 42, COMPOSITE_SCHEMA_DESCRIPTOR, "bit", "qux" ) );
        updater.process( add( 3, COMPOSITE_SCHEMA_DESCRIPTOR, "bit", "bar" ) );
        updater.process( add( 4, COMPOSITE_SCHEMA_DESCRIPTOR, "bit", "baz" ) );
        updater.process( add( 5, COMPOSITE_SCHEMA_DESCRIPTOR, "bit", "bar" ) );
        updater.process( remove( 42, COMPOSITE_SCHEMA_DESCRIPTOR, "bit", "qux" ) );

        verifySamplingResult( sampler, 4, 3, 4 );
    }

    @Test
    public void additionsDeliveredToIndexWriter() throws Exception
    {
        LuceneIndexWriter writer = mock( LuceneIndexWriter.class );
        NonUniqueLuceneIndexPopulatingUpdater updater = newUpdater( writer );

        String expectedString1 = documentRepresentingProperties( 1, "foo" ).toString();
        String expectedString2 = documentRepresentingProperties( 2, "bar" ).toString();
        String expectedString3 = documentRepresentingProperties( 3, "qux" ).toString();
        String expectedString4 = documentRepresentingProperties( 4, "git", "bit" ).toString();

        updater.process( add( 1, SCHEMA_DESCRIPTOR, "foo" ) );
        verifydocument( writer, newTermForChangeOrRemove( 1 ), expectedString1 );

        updater.process( add( 2, SCHEMA_DESCRIPTOR, "bar" ) );
        verifydocument( writer, newTermForChangeOrRemove( 2 ), expectedString2 );

        updater.process( add( 3, SCHEMA_DESCRIPTOR, "qux" ) );
        verifydocument( writer, newTermForChangeOrRemove( 3 ), expectedString3 );

        updater.process( add( 4, COMPOSITE_SCHEMA_DESCRIPTOR, "git", "bit" ) );
        verifydocument( writer, newTermForChangeOrRemove( 4 ), expectedString4 );
    }

    @Test
    public void changesDeliveredToIndexWriter() throws Exception
    {
        LuceneIndexWriter writer = mock( LuceneIndexWriter.class );
        NonUniqueLuceneIndexPopulatingUpdater updater = newUpdater( writer );

        String expectedString1 = documentRepresentingProperties( 1, "after1" ).toString();
        String expectedString2 = documentRepresentingProperties( 2, "after2" ).toString();
        String expectedString3 = documentRepresentingProperties( 3, "bit", "after2" ).toString();

        updater.process( change( 1, SCHEMA_DESCRIPTOR, "before1", "after1" ) );
        verifydocument( writer, newTermForChangeOrRemove( 1 ), expectedString1 );

        updater.process( change( 2, SCHEMA_DESCRIPTOR, "before2", "after2" ) );
        verifydocument( writer, newTermForChangeOrRemove( 2 ), expectedString2 );

        updater.process( change( 3,
                COMPOSITE_SCHEMA_DESCRIPTOR, new Object[]{"bit", "before2"}, new Object[]{"bit", "after2"} ) );
        verifydocument( writer, newTermForChangeOrRemove( 3 ), expectedString3 );
    }

    private void verifydocument( LuceneIndexWriter writer, Term eq, String documentString ) throws IOException
    {
        verify( writer ).updateDocument(  eq(eq), argThat( hasToString( documentString ) ) );
    }

    @Test
    public void removalsDeliveredToIndexWriter() throws Exception
    {
        LuceneIndexWriter writer = mock( LuceneIndexWriter.class );
        NonUniqueLuceneIndexPopulatingUpdater updater = newUpdater( writer );

        updater.process( remove( 1, SCHEMA_DESCRIPTOR, "foo" ) );
        verify( writer ).deleteDocuments( newTermForChangeOrRemove( 1 ) );

        updater.process( remove( 2, SCHEMA_DESCRIPTOR, "bar" ) );
        verify( writer ).deleteDocuments( newTermForChangeOrRemove( 2 ) );

        updater.process( remove( 3, SCHEMA_DESCRIPTOR, "baz" ) );
        verify( writer ).deleteDocuments( newTermForChangeOrRemove( 3 ) );

        updater.process( remove( 4, COMPOSITE_SCHEMA_DESCRIPTOR, "bit", "baz" ) );
        verify( writer ).deleteDocuments( newTermForChangeOrRemove( 4 ) );
    }

    private static void verifySamplingResult( NonUniqueIndexSampler sampler, long expectedIndexSize,
            long expectedUniqueValues, long expectedSampleSize )
    {
        IndexSample sample = sampler.result();

        assertEquals( expectedIndexSize, sample.indexSize() );
        assertEquals( expectedUniqueValues, sample.uniqueValues() );
        assertEquals( expectedSampleSize, sample.sampleSize() );
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
