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
package org.neo4j.kernel.api.impl.schema.reader;

import org.apache.commons.lang3.mutable.MutableInt;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.neo4j.kernel.api.impl.schema.LuceneSchemaIndexBuilder;
import org.neo4j.kernel.api.impl.schema.SchemaIndex;
import org.neo4j.kernel.api.impl.schema.writer.LuceneIndexWriter;
import org.neo4j.kernel.api.index.PropertyAccessor;
import org.neo4j.kernel.api.schema.index.SchemaIndexDescriptorFactory;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.index.schema.GatheringNodeValueClient;
import org.neo4j.storageengine.api.schema.IndexReader;
import org.neo4j.test.rule.RandomRule;
import org.neo4j.test.rule.TestDirectory;
import org.neo4j.test.rule.fs.DefaultFileSystemRule;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.Values;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.neo4j.kernel.api.impl.schema.LuceneDocumentStructure.documentRepresentingProperties;
import static org.neo4j.test.Randoms.CS_DIGITS;
import static org.neo4j.values.storable.Values.stringValue;

public class SimpleIndexReaderDistinctValuesTest
{
    @Rule
    public final RandomRule random = new RandomRule();
    @Rule
    public final DefaultFileSystemRule fs = new DefaultFileSystemRule();
    @Rule
    public final TestDirectory directory = TestDirectory.testDirectory( fs );
    private SchemaIndex index;

    @Before
    public void setup() throws IOException
    {
        index = LuceneSchemaIndexBuilder.create( SchemaIndexDescriptorFactory.forLabel( 1, 1 ), Config.defaults() )
                .withFileSystem( fs )
                .withIndexRootFolder( directory.directory() )
                .build();
        index.create();
        index.open();
    }

    @After
    public void tearDown() throws IOException
    {
        index.close();
    }

    @Test
    public void shouldGetDistinctStringValues() throws IOException
    {
        // given
        LuceneIndexWriter writer = index.getIndexWriter();
        Map<Value,MutableInt> expectedCounts = new HashMap<>();
        for ( int i = 0; i < 10_000; i++ )
        {
            Value value = stringValue( random.randoms().string( 1, 3, CS_DIGITS ) );
            writer.addDocument( documentRepresentingProperties( i, value ) );
            expectedCounts.computeIfAbsent( value, v -> new MutableInt( 0 ) ).increment();
        }
        index.maybeRefreshBlocking();

        // when/then
        GatheringNodeValueClient client = new GatheringNodeValueClient();
        PropertyAccessor propertyAccessor = mock( PropertyAccessor.class );
        try ( IndexReader reader = index.getIndexReader() )
        {
            reader.distinctValues( client, propertyAccessor );
            while ( client.progressor.next() )
            {
                Value value = client.values[0];
                MutableInt expectedCount = expectedCounts.remove( value );
                assertNotNull( expectedCount );
                assertEquals( expectedCount.intValue(), client.reference );
            }
            assertTrue( expectedCounts.isEmpty() );
        }
        verifyNoMoreInteractions( propertyAccessor );
    }

    @Test
    public void shouldCountDistinctValues() throws IOException
    {
        // given
        LuceneIndexWriter writer = index.getIndexWriter();
        int expectedCount = 10_000;
        for ( int i = 0; i < expectedCount; i++ )
        {
            Value value = Values.of( random.propertyValue() );
            writer.addDocument( documentRepresentingProperties( i, value ) );
        }
        index.maybeRefreshBlocking();

        // when/then
        GatheringNodeValueClient client = new GatheringNodeValueClient();
        PropertyAccessor propertyAccessor = mock( PropertyAccessor.class );
        try ( IndexReader reader = index.getIndexReader() )
        {
            reader.distinctValues( client, propertyAccessor );
            int actualCount = 0;
            while ( client.progressor.next() )
            {
                actualCount += client.reference;
            }
            assertEquals( expectedCount, actualCount );
        }
        verifyNoMoreInteractions( propertyAccessor );
    }
}
