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
package org.neo4j.kernel.api.impl.index.verification;

import org.apache.lucene.document.Document;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.search.Collector;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.SearcherFactory;
import org.apache.lucene.search.SearcherManager;
import org.apache.lucene.store.Directory;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;

import org.neo4j.io.IOUtils;
import org.neo4j.kernel.api.exceptions.index.IndexEntryConflictException;
import org.neo4j.kernel.api.impl.index.IndexWriterConfigs;
import org.neo4j.kernel.api.impl.index.TestPropertyAccessor;
import org.neo4j.kernel.api.impl.index.partition.PartitionSearcher;
import org.neo4j.kernel.api.impl.index.storage.DirectoryFactory;
import org.neo4j.kernel.api.impl.schema.LuceneDocumentStructure;
import org.neo4j.kernel.api.impl.schema.verification.SimpleUniquenessVerifier;
import org.neo4j.kernel.api.impl.schema.verification.UniquenessVerifier;
import org.neo4j.kernel.api.index.PropertyAccessor;
import org.neo4j.test.rule.TestDirectory;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.Values;

import static java.util.Arrays.asList;
import static org.hamcrest.Matchers.instanceOf;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.neo4j.kernel.api.impl.LuceneTestUtil.valueTupleList;

public class SimpleUniquenessVerifierTest
{
    private static final int[] PROPERTY_KEY_IDS = new int[]{42};

    @Rule
    public TestDirectory testDir = TestDirectory.testDirectory();

    private DirectoryFactory dirFactory;
    private IndexWriter writer;
    private SearcherManager searcherManager;

    @Before
    public void initLuceneResources() throws Exception
    {
        dirFactory = new DirectoryFactory.InMemoryDirectoryFactory();
        Directory dir = dirFactory.open( testDir.directory( "test" ) );
        writer = new IndexWriter( dir, IndexWriterConfigs.standard() );
        searcherManager = new SearcherManager( writer, true, new SearcherFactory() );
    }

    @After
    public void closeLuceneResources() throws Exception
    {
        IOUtils.closeAll( searcherManager, writer, dirFactory );
    }

    @Test
    public void partitionSearcherIsClosed() throws IOException
    {
        PartitionSearcher partitionSearcher = mock( PartitionSearcher.class );
        SimpleUniquenessVerifier verifier = new SimpleUniquenessVerifier( partitionSearcher );

        verifier.close();

        verify( partitionSearcher ).close();
    }

    @Test
    public void populationVerificationNoDuplicates() throws Exception
    {
        List<Object> data = asList( "string1", 42, 43, 44, 45L, (byte) 46, 47.0, (float) 48.1, "string2" );
        PropertyAccessor propertyAccessor = newPropertyAccessor( data );

        insert( data );

        assertNoDuplicates( propertyAccessor );
    }

    @Test
    public void populationVerificationOneDuplicate() throws IOException
    {
        List<Object> data = asList( "cat", 21, 22, 23, 24L, (byte) 25, 26.0, (float) 22, "dog" );
        PropertyAccessor propertyAccessor = newPropertyAccessor( data );

        insert( data );

        assertHasDuplicates( propertyAccessor );
    }

    @Test
    public void populationVerificationManyDuplicate() throws IOException
    {
        List<Object> data = asList( "dog", "cat", "dog", "dog", "dog", "dog" );
        PropertyAccessor propertyAccessor = newPropertyAccessor( data );

        insert( data );

        assertHasDuplicates( propertyAccessor );
    }

    @Test
    public void updatesVerificationNoDuplicates() throws Exception
    {
        List<Object> data = asList( "lucene", 1337975550, 43.10, 'a', 'b', 'c', (byte) 12 );
        PropertyAccessor propertyAccessor = newPropertyAccessor( data );

        insert( data );

        assertNoDuplicatesCreated( propertyAccessor, valueTupleList( 1337975550, 'c', (byte) 12 ) );
    }

    @Test
    public void updatesVerificationOneDuplicate() throws IOException
    {
        List<Object> data = asList( "foo", "bar", "baz", 100, 200, 'q', 'u', 'x', "aa", 300, 'u', -100 );
        PropertyAccessor propertyAccessor = newPropertyAccessor( data );

        insert( data );

        assertDuplicatesCreated( propertyAccessor, valueTupleList( "aa", 'u', -100 ) );
    }

    @Test
    public void updatesVerificationManyDuplicate() throws IOException
    {
        List<Object> data = asList( -99, 'a', -10.0, -99.99999, "apa", (float) -99.99999, "mod", "div", "div", -10 );
        PropertyAccessor propertyAccessor = newPropertyAccessor( data );

        insert( data );

        assertDuplicatesCreated( propertyAccessor, valueTupleList( (float) -99.99999, 'a', -10, "div" ) );
    }

    @Test
    public void numericIndexVerificationNoDuplicates() throws Exception
    {
        List<Object> data = asList( Integer.MAX_VALUE - 2, Integer.MAX_VALUE - 1, Integer.MAX_VALUE );
        PropertyAccessor propertyAccessor = newPropertyAccessor( data );

        insert( data );

        IndexSearcher indexSearcher = spy( searcherManager.acquire() );
        runUniquenessVerification( propertyAccessor, indexSearcher );

        verify( indexSearcher, never() ).search( any( Query.class ), any( Collector.class ) );
    }

    @Test
    public void numericIndexVerificationSomePossibleDuplicates() throws Exception
    {
        List<Object> data = asList( 42, Long.MAX_VALUE - 1, Long.MAX_VALUE );
        PropertyAccessor propertyAccessor = newPropertyAccessor( data );

        insert( data );

        IndexSearcher indexSearcher = spy( searcherManager.acquire() );
        runUniquenessVerification( propertyAccessor, indexSearcher );

        verify( indexSearcher ).search( any( Query.class ), any( Collector.class ) );
    }

    @Test
    public void numericIndexVerificationSomeWithDuplicates() throws Exception
    {
        List<Object> data = asList( Integer.MAX_VALUE, Long.MAX_VALUE, 42, Long.MAX_VALUE );
        PropertyAccessor propertyAccessor = newPropertyAccessor( data );

        insert( data );

        IndexSearcher indexSearcher = spy( searcherManager.acquire() );
        try
        {
            runUniquenessVerification( propertyAccessor, indexSearcher );
            fail( "Exception expected" );
        }
        catch ( Throwable t )
        {
            assertThat( t, instanceOf( IndexEntryConflictException.class ) );
        }

        verify( indexSearcher ).search( any( Query.class ), any( Collector.class ) );
    }

    private void runUniquenessVerification( PropertyAccessor propertyAccessor, IndexSearcher indexSearcher )
            throws IOException, IndexEntryConflictException
    {
        try
        {
            PartitionSearcher partitionSearcher = mock( PartitionSearcher.class );
            when( partitionSearcher.getIndexSearcher() ).thenReturn( indexSearcher );

            try ( UniquenessVerifier verifier = new SimpleUniquenessVerifier( partitionSearcher ) )
            {
                verifier.verify( propertyAccessor, PROPERTY_KEY_IDS );
            }
        }
        finally
        {
            searcherManager.release( indexSearcher );
        }
    }

    private void assertNoDuplicates( PropertyAccessor propertyAccessor ) throws Exception
    {
        try ( UniquenessVerifier verifier = newSimpleUniquenessVerifier() )
        {
            verifier.verify( propertyAccessor, PROPERTY_KEY_IDS );
        }
    }

    private void assertNoDuplicatesCreated( PropertyAccessor propertyAccessor, List<Value[]> updatedPropertyValues )
            throws Exception
    {
        try ( UniquenessVerifier verifier = newSimpleUniquenessVerifier() )
        {
            verifier.verify( propertyAccessor, PROPERTY_KEY_IDS, updatedPropertyValues );
        }
    }

    private void assertHasDuplicates( PropertyAccessor propertyAccessor )
    {
        try ( UniquenessVerifier verifier = newSimpleUniquenessVerifier() )
        {
            verifier.verify( propertyAccessor, PROPERTY_KEY_IDS );
            fail( "Uniqueness verification was successful. This is not expected..." );
        }
        catch ( Throwable t )
        {
            assertThat( t, instanceOf( IndexEntryConflictException.class ) );
        }
    }

    private void assertDuplicatesCreated( PropertyAccessor propertyAccessor, List<Value[]> updatedPropertyValues )
    {
        try ( UniquenessVerifier verifier = newSimpleUniquenessVerifier() )
        {
            verifier.verify( propertyAccessor, PROPERTY_KEY_IDS, updatedPropertyValues );
            fail( "Uniqueness verification was successful. This is not expected..." );
        }
        catch ( Throwable t )
        {
            assertThat( t, instanceOf( IndexEntryConflictException.class ) );
        }
    }

    private void insert( List<Object> data ) throws IOException
    {
        for ( int i = 0; i < data.size(); i++ )
        {
            Document doc = LuceneDocumentStructure.documentRepresentingProperties( i, Values.of( data.get( i ) ) );
            writer.addDocument( doc );
        }
        searcherManager.maybeRefreshBlocking();
    }

    private PropertyAccessor newPropertyAccessor( List<Object> propertyValues )
    {
        return new TestPropertyAccessor(
                propertyValues.stream()
                        .map( Values::of )
                        .collect( Collectors.toList() ) );
    }

    private UniquenessVerifier newSimpleUniquenessVerifier() throws IOException
    {
        PartitionSearcher partitionSearcher = new PartitionSearcher( searcherManager );
        return new SimpleUniquenessVerifier( partitionSearcher );
    }
}
