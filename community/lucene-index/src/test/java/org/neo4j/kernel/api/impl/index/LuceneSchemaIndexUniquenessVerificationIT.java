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
import org.apache.lucene.document.Document;
import org.apache.lucene.index.IndexWriterConfig;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.IntStream;

import org.neo4j.function.Factory;
import org.neo4j.helpers.ArrayUtil;
import org.neo4j.helpers.Exceptions;
import org.neo4j.helpers.Strings;
import org.neo4j.io.IOUtils;
import org.neo4j.kernel.api.exceptions.index.IndexEntryConflictException;
import org.neo4j.kernel.api.impl.schema.LuceneDocumentStructure;
import org.neo4j.kernel.api.impl.schema.LuceneSchemaIndexBuilder;
import org.neo4j.kernel.api.impl.schema.SchemaIndex;
import org.neo4j.kernel.api.index.PropertyAccessor;
import org.neo4j.kernel.api.properties.Property;
import org.neo4j.kernel.api.schema.index.IndexDescriptor;
import org.neo4j.kernel.api.schema.index.IndexDescriptorFactory;
import org.neo4j.test.Randoms;
import org.neo4j.test.rule.TestDirectory;
import org.neo4j.test.rule.fs.DefaultFileSystemRule;

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

    private int nodesToCreate = DOCS_PER_PARTITION * 2 + 1;

    private SchemaIndex index;
    private static final long MAX_LONG_VALUE = Long.MAX_VALUE >> 10;
    private static final long MIN_LONG_VALUE = MAX_LONG_VALUE - 20;

    @Before
    public void setPartitionSize() throws Exception
    {
        System.setProperty( "luceneSchemaIndex.maxPartitionSize", String.valueOf( DOCS_PER_PARTITION ) );

        Factory<IndexWriterConfig> configFactory = new VerboseConfigFactory();
        index = LuceneSchemaIndexBuilder.create( descriptor )
                .withFileSystem( fileSystemRule.get() )
                .withIndexRootFolder( testDir.directory( "uniquenessVerification" ) )
                .withWriterConfig( configFactory )
                .withIndexIdentifier( "index" )
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
        Set<PropertyValue> data = randomStrings();

        insert( data );

        assertUniquenessConstraintHolds( data );
    }

    @Test
    public void stringValuesWithDuplicates() throws IOException
    {
        List<PropertyValue> data = withDuplicate( randomStrings() );

        insert( data );

        assertUniquenessConstraintFails( data );
    }

    @Test
    public void smallLongValuesWithoutDuplicates() throws IOException
    {
        long min = randomLongInRange( 100, 10_000 );
        long max = min + nodesToCreate;
        Set<PropertyValue> data = randomLongs( min, max );

        insert( data );

        assertUniquenessConstraintHolds( data );
    }

    @Test
    public void smallLongValuesWithDuplicates() throws IOException
    {
        long min = randomLongInRange( 100, 10_000 );
        long max = min + nodesToCreate;
        List<PropertyValue> data = withDuplicate( randomLongs( min, max ) );

        insert( data );

        assertUniquenessConstraintFails( data );
    }

    @Test
    public void largeLongValuesWithoutDuplicates() throws IOException
    {
        long max = randomLongInRange( MIN_LONG_VALUE, MAX_LONG_VALUE );
        long min = max - nodesToCreate;
        Set<PropertyValue> data = randomLongs( min, max );

        insert( data );

        assertUniquenessConstraintHolds( data );
    }

    @Test
    public void largeLongValuesWithDuplicates() throws IOException
    {
        long max = randomLongInRange( MIN_LONG_VALUE, MAX_LONG_VALUE );
        long min = max - nodesToCreate;
        List<PropertyValue> data = withDuplicate( randomLongs( min, max ) );

        insert( data );

        assertUniquenessConstraintFails( data );
    }

    @Test
    public void smallDoubleValuesWithoutDuplicates() throws IOException
    {
        double min = randomDoubleInRange( 100, 10_000 );
        double max = min + nodesToCreate;
        Set<PropertyValue> data = randomDoubles( min, max );

        insert( data );

        assertUniquenessConstraintHolds( data );
    }

    @Test
    public void smallDoubleValuesWithDuplicates() throws IOException
    {
        double min = randomDoubleInRange( 100, 10_000 );
        double max = min + nodesToCreate;
        List<PropertyValue> data = withDuplicate( randomDoubles( min, max ) );

        insert( data );

        assertUniquenessConstraintFails( data );
    }

    @Test
    public void largeDoubleValuesWithoutDuplicates() throws IOException
    {
        double max = randomDoubleInRange( Double.MAX_VALUE / 2, Double.MAX_VALUE );
        double min = max / 2;
        Set<PropertyValue> data = randomDoubles( min, max );

        insert( data );

        assertUniquenessConstraintHolds( data );
    }

    @Test
    public void largeDoubleValuesWithDuplicates() throws IOException
    {
        double max = randomDoubleInRange( Double.MAX_VALUE / 2, Double.MAX_VALUE );
        double min = max / 2;
        List<PropertyValue> data = withDuplicate( randomDoubles( min, max ) );

        insert( data );

        assertUniquenessConstraintFails( data );
    }

    @Test
    public void smallArrayValuesWithoutDuplicates() throws IOException
    {
        Set<PropertyValue> data = randomArrays( 3, 7 );

        insert( data );

        assertUniquenessConstraintHolds( data );
    }

    @Test
    public void smallArrayValuesWithDuplicates() throws IOException
    {
        List<PropertyValue> data = withDuplicate( randomArrays( 3, 7 ) );

        insert( data );

        assertUniquenessConstraintFails( data );
    }

    @Test
    public void largeArrayValuesWithoutDuplicates() throws IOException
    {
        Set<PropertyValue> data = randomArrays( 70, 100 );

        insert( data );

        assertUniquenessConstraintHolds( data );
    }

    @Test
    public void largeArrayValuesWithDuplicates() throws IOException
    {
        List<PropertyValue> data = withDuplicate( randomArrays( 70, 100 ) );

        insert( data );

        assertUniquenessConstraintFails( data );
    }

    @Test
    public void variousValuesWithoutDuplicates() throws IOException
    {
        Set<PropertyValue> data = randomPropertyValues();

        insert( data );

        assertUniquenessConstraintHolds( data );
    }

    @Test
    public void variousValuesWitDuplicates() throws IOException
    {
        List<PropertyValue> data = withDuplicate( randomPropertyValues() );

        insert( data );

        assertUniquenessConstraintFails( data );
    }

    private void insert( Collection<PropertyValue> data ) throws IOException
    {
        PropertyValue[] dataArray = data.toArray( new PropertyValue[data.size()] );
        for ( int i = 0; i < dataArray.length; i++ )
        {
            Document doc = LuceneDocumentStructure.documentRepresentingProperties( i, dataArray[i].value );
            index.getIndexWriter().addDocument( doc );
        }
        index.maybeRefreshBlocking();
    }

    private void assertUniquenessConstraintHolds( Collection<PropertyValue> data )
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

    private void assertUniquenessConstraintFails( Collection<PropertyValue> data )
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

    private void verifyUniqueness( Collection<PropertyValue> data ) throws IOException, IndexEntryConflictException
    {
        Object[] propertyValues = data.stream().map( property -> property.value ).toArray();
        PropertyAccessor propertyAccessor = new TestPropertyAccessor( propertyValues );
        index.verifyUniqueness( propertyAccessor, new int[]{PROPERTY_KEY_ID} );
    }

    private Set<PropertyValue> randomStrings()
    {
        return ThreadLocalRandom.current()
                .ints( nodesToCreate, 1, 200 )
                .mapToObj( this::randomString )
                .map( PropertyValue::new )
                .collect( toSet() );
    }

    private String randomString( int size )
    {
        return ThreadLocalRandom.current().nextBoolean()
               ? RandomStringUtils.random( size )
               : RandomStringUtils.randomAlphabetic( size );
    }

    private Set<PropertyValue> randomLongs( long min, long max )
    {
        return ThreadLocalRandom.current()
                .longs( nodesToCreate, min, max )
                .boxed()
                .map( PropertyValue::new )
                .collect( toSet() );
    }

    private Set<PropertyValue> randomDoubles( double min, double max )
    {
        return ThreadLocalRandom.current()
                .doubles( nodesToCreate, min, max )
                .boxed()
                .map( PropertyValue::new )
                .collect( toSet() );
    }

    private Set<PropertyValue> randomArrays( int minLength, int maxLength )
    {
        Randoms randoms = new Randoms( ThreadLocalRandom.current(), new ArraySizeConfig( minLength, maxLength ) );

        return IntStream.range( 0, nodesToCreate )
                .mapToObj( i -> randoms.array() )
                .map( PropertyValue::new )
                .collect( toSet() );
    }

    private Set<PropertyValue> randomPropertyValues()
    {
        Randoms randoms = new Randoms( ThreadLocalRandom.current(), new ArraySizeConfig( 5, 100 ) );

        return IntStream.range( 0, nodesToCreate )
                .mapToObj( i -> randoms.propertyValue() )
                .map( PropertyValue::new )
                .collect( toSet() );
    }

    private static List<PropertyValue> withDuplicate( Set<PropertyValue> set )
    {
        List<PropertyValue> data = new ArrayList<>( set );
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
            PropertyValue duplicate = duplicatePropertyValue( data.get( duplicateValueIndex ) );
            data.set( duplicateIndex, duplicate );
        }
        return data;
    }

    private static PropertyValue duplicatePropertyValue( PropertyValue propertyValue )
    {
        return new PropertyValue( propertyValue.value );
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

    /**
     * This class is used to implement correct equals and hashCode for property values of numeric and array types.
     */
    private static class PropertyValue
    {
        final Object value;

        PropertyValue( Object value )
        {
            this.value = Objects.requireNonNull( value );
        }

        @Override
        public boolean equals( Object o )
        {
            if ( this == o )
            {
                return true;
            }
            if ( o == null || getClass() != o.getClass() )
            {
                return false;
            }
            PropertyValue that = (PropertyValue) o;
            return Property.property( PROPERTY_KEY_ID, value ).valueEquals( that.value );
        }

        @Override
        public int hashCode()
        {
            if ( value instanceof Number )
            {
                return Double.hashCode( ((Number) value).doubleValue() );
            }
            else if ( value.getClass().isArray() )
            {
                return ArrayUtil.hashCode( value );
            }
            return value.hashCode();
        }

        @Override
        public String toString()
        {
            return Strings.prettyPrint( value );
        }
    }

    private static class VerboseConfigFactory implements Factory<IndexWriterConfig>
    {

        @Override
        public IndexWriterConfig newInstance()
        {
            IndexWriterConfig verboseConfig = IndexWriterConfigs.standard();
            verboseConfig.setInfoStream( System.out );
            return verboseConfig;
        }
    }
}
