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
package org.neo4j.kernel.api.impl.index;

import org.apache.commons.lang3.RandomStringUtils;
import org.apache.lucene.codecs.Codec;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.IndexWriterConfig;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;

import org.neo4j.configuration.Config;
import org.neo4j.function.Factory;
import org.neo4j.internal.helpers.Exceptions;
import org.neo4j.internal.helpers.Strings;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.io.IOUtils;
import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.kernel.api.exceptions.index.IndexEntryConflictException;
import org.neo4j.kernel.api.impl.index.storage.DirectoryFactory;
import org.neo4j.kernel.api.impl.schema.LuceneDocumentStructure;
import org.neo4j.kernel.api.impl.schema.LuceneSchemaIndexBuilder;
import org.neo4j.kernel.api.impl.schema.SchemaIndex;
import org.neo4j.kernel.api.schema.index.TestIndexDescriptorFactory;
import org.neo4j.storageengine.api.NodePropertyAccessor;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.testdirectory.TestDirectoryExtension;
import org.neo4j.test.rule.TestDirectory;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.Values;

import static java.util.stream.Collectors.toSet;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.fail;

@TestDirectoryExtension
class LuceneSchemaIndexUniquenessVerificationIT
{
    private static final int DOCS_PER_PARTITION = ThreadLocalRandom.current().nextInt( 10, 100 );
    private static final int PROPERTY_KEY_ID = 42;
    private static final IndexDescriptor descriptor = TestIndexDescriptorFactory.uniqueForLabel( 0, PROPERTY_KEY_ID );
    private static final int nodesToCreate = DOCS_PER_PARTITION * 2 + 1;
    private static final long MAX_LONG_VALUE = Long.MAX_VALUE >> 10;
    private static final long MIN_LONG_VALUE = MAX_LONG_VALUE - 20;

    @Inject
    private TestDirectory testDir;
    @Inject
    private DefaultFileSystemAbstraction fileSystem;

    private SchemaIndex index;

    @BeforeEach
    void setPartitionSize() throws Exception
    {
        System.setProperty( "luceneSchemaIndex.maxPartitionSize", String.valueOf( DOCS_PER_PARTITION ) );

        Factory<IndexWriterConfig> configFactory = new TestConfigFactory();
        index = LuceneSchemaIndexBuilder.create( descriptor, Config.defaults() )
                .withFileSystem( fileSystem )
                .withIndexRootFolder( new File( testDir.directory( "uniquenessVerification" ), "index" ) )
                .withWriterConfig( configFactory )
                .withDirectoryFactory( DirectoryFactory.PERSISTENT )
                .build();

        index.create();
        index.open();
    }

    @AfterEach
    void resetPartitionSize() throws IOException
    {
        System.setProperty( "luceneSchemaIndex.maxPartitionSize", "" );

        IOUtils.closeAll( index );
    }

    @Test
    void stringValuesWithoutDuplicates() throws IOException
    {
        Set<Value> data = randomStrings();

        insert( data );

        assertUniquenessConstraintHolds( data );
    }

    @Test
    void stringValuesWithDuplicates() throws IOException
    {
        List<Value> data = withDuplicate( randomStrings() );

        insert( data );

        assertUniquenessConstraintFails( data );
    }

    private void insert( Collection<Value> data ) throws IOException
    {
        Value[] dataArray = data.toArray( new Value[0] );
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
        assertThrows( IndexEntryConflictException.class, () -> verifyUniqueness( data ) );
    }

    private void verifyUniqueness( Collection<Value> data ) throws IOException, IndexEntryConflictException
    {
        NodePropertyAccessor nodePropertyAccessor = new TestPropertyAccessor( new ArrayList<>( data ) );
        index.verifyUniqueness( nodePropertyAccessor, new int[]{PROPERTY_KEY_ID} );
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
