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
package org.neo4j.kernel.api.impl.index;

import org.apache.commons.lang3.RandomStringUtils;
import org.apache.lucene.codecs.Codec;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.IndexWriterConfig;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.IntStream;

import org.neo4j.function.Factory;
import org.neo4j.helpers.Exceptions;
import org.neo4j.helpers.Strings;
import org.neo4j.io.IOUtils;
import org.neo4j.kernel.api.exceptions.index.IndexEntryConflictException;
import org.neo4j.kernel.api.impl.index.storage.DirectoryFactory;
import org.neo4j.kernel.api.impl.schema.LuceneDocumentStructure;
import org.neo4j.kernel.api.impl.schema.LuceneSchemaIndexBuilder;
import org.neo4j.kernel.api.impl.schema.SchemaIndex;
import org.neo4j.kernel.api.index.PropertyAccessor;
import org.neo4j.kernel.api.schema.index.IndexDescriptor;
import org.neo4j.kernel.api.schema.index.IndexDescriptorFactory;
import org.neo4j.test.Randoms;
import org.neo4j.test.rule.TestDirectory;
import org.neo4j.test.rule.fs.DefaultFileSystemRule;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.Values;

import static java.util.stream.Collectors.toSet;
import static org.hamcrest.Matchers.instanceOf;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

public class LuceneSchemaIndexUniquenessVerificationIT
{
    private static final int DOCS_PER_PARTITION = ThreadLocalRandom.current().nextInt( 10, 100 );
    private static final int PROPERTY_KEY_ID = 42;
    private static final IndexDescriptor descriptor = IndexDescriptorFactory.uniqueForLabel( 0, PROPERTY_KEY_ID );

    @Rule
    public TestDirectory testDir = TestDirectory.testDirectory();
    @Rule
    public final DefaultFileSystemRule fileSystemRule = new DefaultFileSystemRule();

    private final int nodesToCreate = DOCS_PER_PARTITION * 2 + 1;

    private SchemaIndex index;
    private static final long MAX_LONG_VALUE = Long.MAX_VALUE >> 10;
    private static final long MIN_LONG_VALUE = MAX_LONG_VALUE - 20;

    @Before
    public void setPartitionSize() throws Exception
    {
        System.setProperty( "luceneSchemaIndex.maxPartitionSize", String.valueOf( DOCS_PER_PARTITION ) );

        Factory<IndexWriterConfig> configFactory = new TestConfigFactory();
        index = LuceneSchemaIndexBuilder.create( descriptor )
                .withFileSystem( fileSystemRule.get() )
                .withIndexRootFolder( new File( testDir.directory( "uniquenessVerification" ), "index" ) )
                .withWriterConfig( configFactory )
                .withDirectoryFactory( DirectoryFactory.PERSISTENT )
                .build();

        index.create();
        index.open();
    }

    @After
    public void resetPartitionSize() throws IOException
    {
        System.setProperty( "luceneSchemaIndex.maxPartitionSize", "" );

        IOUtils.closeAll( index );
    }

    @Test
    public void stringValuesWithoutDuplicates() throws IOException
    {
        Set<Value> data = randomStrings();

        insert( data );

        assertUniquenessConstraintHolds( data );
    }

    @Test
    public void stringValuesWithDuplicates() throws IOException
    {
        List<Value> data = withDuplicate( randomStrings() );

        insert( data );

        assertUniquenessConstraintFails( data );
    }

    @Test
    public void smallLongValuesWithoutDuplicates() throws IOException
    {
        long min = randomLongInRange( 100, 10_000 );
        long max = min + nodesToCreate;
        Set<Value> data = randomLongs( min, max );

        insert( data );

        assertUniquenessConstraintHolds( data );
    }

    @Test
    public void smallLongValuesWithDuplicates() throws IOException
    {
        long min = randomLongInRange( 100, 10_000 );
        long max = min + nodesToCreate;
        List<Value> data = withDuplicate( randomLongs( min, max ) );

        insert( data );

        assertUniquenessConstraintFails( data );
    }

    @Test
    public void largeLongValuesWithoutDuplicates() throws IOException
    {
        long max = randomLongInRange( MIN_LONG_VALUE, MAX_LONG_VALUE );
        long min = max - nodesToCreate;
        Set<Value> data = randomLongs( min, max );

        insert( data );

        assertUniquenessConstraintHolds( data );
    }

    @Test
    public void largeLongValuesWithDuplicates() throws IOException
    {
        long max = randomLongInRange( MIN_LONG_VALUE, MAX_LONG_VALUE );
        long min = max - nodesToCreate;
        List<Value> data = withDuplicate( randomLongs( min, max ) );

        insert( data );

        assertUniquenessConstraintFails( data );
    }

    @Test
    public void smallDoubleValuesWithoutDuplicates() throws IOException
    {
        double min = randomDoubleInRange( 100, 10_000 );
        double max = min + nodesToCreate;
        Set<Value> data = randomDoubles( min, max );

        insert( data );

        assertUniquenessConstraintHolds( data );
    }

    @Test
    public void smallDoubleValuesWithDuplicates() throws IOException
    {
        double min = randomDoubleInRange( 100, 10_000 );
        double max = min + nodesToCreate;
        List<Value> data = withDuplicate( randomDoubles( min, max ) );

        insert( data );

        assertUniquenessConstraintFails( data );
    }

    @Test
    public void largeDoubleValuesWithoutDuplicates() throws IOException
    {
        double max = randomDoubleInRange( Double.MAX_VALUE / 2, Double.MAX_VALUE );
        double min = max / 2;
        Set<Value> data = randomDoubles( min, max );

        insert( data );

        assertUniquenessConstraintHolds( data );
    }

    @Test
    public void largeDoubleValuesWithDuplicates() throws IOException
    {
        double max = randomDoubleInRange( Double.MAX_VALUE / 2, Double.MAX_VALUE );
        double min = max / 2;
        List<Value> data = withDuplicate( randomDoubles( min, max ) );

        insert( data );

        assertUniquenessConstraintFails( data );
    }

    @Test
    public void smallArrayValuesWithoutDuplicates() throws IOException
    {
        Set<Value> data = randomArrays( 3, 7 );

        insert( data );

        assertUniquenessConstraintHolds( data );
    }

    @Test
    public void smallArrayValuesWithDuplicates() throws IOException
    {
        List<Value> data = withDuplicate( randomArrays( 3, 7 ) );

        insert( data );

        assertUniquenessConstraintFails( data );
    }

    @Test
    public void largeArrayValuesWithoutDuplicates() throws IOException
    {
        Set<Value> data = randomArrays( 70, 100 );

        insert( data );

        assertUniquenessConstraintHolds( data );
    }

    @Test
    public void largeArrayValuesWithDuplicates() throws IOException
    {
        List<Value> data = withDuplicate( randomArrays( 70, 100 ) );

        insert( data );

        assertUniquenessConstraintFails( data );
    }

    @Test
    public void variousValuesWithoutDuplicates() throws IOException
    {
        Set<Value> data = randomValues();

        insert( data );

        assertUniquenessConstraintHolds( data );
    }

    @Test
    public void variousValuesWitDuplicates() throws IOException
    {
        List<Value> data = withDuplicate( randomValues() );

        insert( data );

        assertUniquenessConstraintFails( data );
    }

    private void insert( Collection<Value> data ) throws IOException
    {
        Value[] dataArray = data.toArray( new Value[data.size()] );
        for ( int i = 0; i < dataArray.length; i++ )
        {
            Document doc = LuceneDocumentStructure.documentRepresentingProperties( i, dataArray[i] );
            index.getIndexWriter().addDocument( doc );
        }
        index.maybeRefreshBlocking();
    }

    private void assertUniquenessConstraintHolds( Collection<Value> data )
    {
        try
        {
            verifyUniqueness( data );
        }
        catch ( Throwable t )
        {
            fail( "Unable to create uniqueness constraint for data: " + Strings.prettyPrint( data.toArray() ) +
                  "\n" + Exceptions.stringify( t ) );
        }
    }

    private void assertUniquenessConstraintFails( Collection<Value> data )
    {
        try
        {
            verifyUniqueness( data );
            fail( "Should not be possible to create uniqueness constraint for data: " +
                  Strings.prettyPrint( data.toArray() ) );
        }
        catch ( Throwable t )
        {
            assertThat( t, instanceOf( IndexEntryConflictException.class ) );
        }
    }

    private void verifyUniqueness( Collection<Value> data ) throws IOException, IndexEntryConflictException
    {
        PropertyAccessor propertyAccessor = new TestPropertyAccessor( new ArrayList<>( data ) );
        index.verifyUniqueness( propertyAccessor, new int[]{PROPERTY_KEY_ID} );
    }

    private Set<Value> randomStrings()
    {
        return ThreadLocalRandom.current()
                .ints( nodesToCreate, 1, 200 )
                .mapToObj( this::randomString )
                .map( Values::of )
                .collect( toSet() );
    }

    private String randomString( int size )
    {
        return ThreadLocalRandom.current().nextBoolean()
               ? RandomStringUtils.random( size )
               : RandomStringUtils.randomAlphabetic( size );
    }

    private Set<Value> randomLongs( long min, long max )
    {
        return ThreadLocalRandom.current()
                .longs( nodesToCreate, min, max )
                .boxed()
                .map( Values::of )
                .collect( toSet() );
    }

    private Set<Value> randomDoubles( double min, double max )
    {
        return ThreadLocalRandom.current()
                .doubles( nodesToCreate, min, max )
                .boxed()
                .map( Values::of )
                .collect( toSet() );
    }

    private Set<Value> randomArrays( int minLength, int maxLength )
    {
        Randoms randoms = new Randoms( ThreadLocalRandom.current(), new ArraySizeConfig( minLength, maxLength ) );

        return IntStream.range( 0, nodesToCreate )
                .mapToObj( i -> randoms.array() )
                .map( Values::of )
                .collect( toSet() );
    }

    private Set<Value> randomValues()
    {
        Randoms randoms = new Randoms( ThreadLocalRandom.current(), new ArraySizeConfig( 5, 100 ) );

        return IntStream.range( 0, nodesToCreate )
                .mapToObj( i -> randoms.propertyValue() )
                .map( Values::of )
                .collect( toSet() );
    }

    private static List<Value> withDuplicate( Set<Value> set )
    {
        List<Value> data = new ArrayList<>( set );
        if ( data.isEmpty() )
        {
            throw new IllegalStateException();
        }
        else if ( data.size() == 1 )
        {
            data.add( data.get( 0 ) );
        }
        else
        {
            int duplicateIndex = randomIntInRange( 0, data.size() );
            int duplicateValueIndex;
            do
            {
                duplicateValueIndex = ThreadLocalRandom.current().nextInt( data.size() );
            }
            while ( duplicateValueIndex == duplicateIndex );
            Value duplicate = duplicateValue( data.get( duplicateValueIndex ) );
            data.set( duplicateIndex, duplicate );
        }
        return data;
    }

    private static Value duplicateValue( Value propertyValue )
    {
        return Values.of( propertyValue.asObjectCopy() );
    }

    private static int randomIntInRange( int min, int max )
    {
        return ThreadLocalRandom.current().nextInt( min, max );
    }

    private static long randomLongInRange( long min, long max )
    {
        return ThreadLocalRandom.current().nextLong( min, max );
    }

    private static double randomDoubleInRange( double min, double max )
    {
        return ThreadLocalRandom.current().nextDouble( min, max );
    }

    private static class ArraySizeConfig extends Randoms.Default
    {
        final int minLength;
        final int maxLength;

        ArraySizeConfig( int minLength, int maxLength )
        {
            this.minLength = minLength;
            this.maxLength = maxLength;
        }

        @Override
        public int arrayMinLength()
        {
            return super.arrayMinLength();
        }

        @Override
        public int arrayMaxLength()
        {
            return super.arrayMaxLength();
        }
    }

    private static class TestConfigFactory implements Factory<IndexWriterConfig>
    {

        @Override
        public IndexWriterConfig newInstance()
        {
            IndexWriterConfig verboseConfig = IndexWriterConfigs.standard();
            verboseConfig.setCodec( Codec.getDefault() );
            return verboseConfig;
        }
    }
}
