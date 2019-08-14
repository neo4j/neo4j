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
package org.neo4j.kernel.api.index;

import org.junit.Ignore;
import org.junit.Test;

import org.neo4j.internal.schema.IndexCapability;
import org.neo4j.internal.schema.IndexConfig;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.internal.schema.IndexOrder;
import org.neo4j.internal.schema.IndexPrototype;
import org.neo4j.internal.schema.IndexValueCapability;
import org.neo4j.internal.schema.SchemaDescriptor;
import org.neo4j.values.storable.ValueCategory;
import org.neo4j.values.storable.Values;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertSame;

@Ignore( "Not a test. This is a compatibility suite that provides test cases for verifying" +
        " IndexProvider implementations. Each index provider that is to be tested by this suite" +
        " must create their own test class extending IndexProviderCompatibilityTestSuite." +
        " The @Ignore annotation doesn't prevent these tests to run, it rather removes some annoying" +
        " errors or warnings in some IDEs about test classes needing a public zero-arg constructor." )
public class IndexConfigurationCompletionCompatibility extends IndexProviderCompatibilityTestSuite.Compatibility
{
    public IndexConfigurationCompletionCompatibility( IndexProviderCompatibilityTestSuite testSuite )
    {
        super( testSuite, IndexPrototype.uniqueForSchema( SchemaDescriptor.forLabel( 1, 2 ) ) );
    }

    @Test
    public void configurationCompletionMustNotOverwriteExistingConfiguration()
    {
        SchemaDescriptor schema = SchemaDescriptor.forLabel( 1, 1 );
        schema = schema.withIndexConfig( IndexConfig.empty().withIfAbsent( "Bob", Values.stringValue( "Howard" ) ) );
        IndexDescriptor index = IndexPrototype.forSchema( schema ).withName( "index" ).materialise( 0 );
        index = indexProvider.completeConfiguration( index );
        assertEquals( index.schema().getIndexConfig().get( "Bob" ), Values.stringValue( "Howard" ) );
    }

    @Test
    public void configurationCompletionMustBeIdempotent()
    {
        SchemaDescriptor schema = SchemaDescriptor.forLabel( 1, 1 );
        IndexDescriptor index = IndexPrototype.forSchema( schema ).withName( "index" ).materialise( 0 );
        IndexDescriptor onceCompleted = indexProvider.completeConfiguration( index );
        IndexDescriptor twiceCompleted = indexProvider.completeConfiguration( onceCompleted );
        assertEquals( onceCompleted.schema().getIndexConfig(), twiceCompleted.schema().getIndexConfig() );
    }

    @Test
    public void mustAssignCapabilitiesToDescriptorsThatHaveNone()
    {
        SchemaDescriptor schema = SchemaDescriptor.forLabel( 1, 1 );
        IndexDescriptor index = IndexPrototype.forSchema( schema ).withName( "index" ).materialise( 0 );
        IndexDescriptor completed = indexProvider.completeConfiguration( index );
        assertNotEquals( completed.getCapability(), IndexCapability.NO_CAPABILITY );
        completed = completed.withIndexCapability( IndexCapability.NO_CAPABILITY );
        completed = indexProvider.completeConfiguration( completed );
        assertNotEquals( completed.getCapability(), IndexCapability.NO_CAPABILITY );
    }

    @Test
    public void mustNotOverwriteExistingCapabilities()
    {
        IndexCapability capability = new IndexCapability()
        {
            @Override
            public IndexOrder[] orderCapability( ValueCategory... valueCategories )
            {
                return new IndexOrder[0];
            }

            @Override
            public IndexValueCapability valueCapability( ValueCategory... valueCategories )
            {
                return IndexValueCapability.NO;
            }
        };
        SchemaDescriptor schema = SchemaDescriptor.forLabel( 1, 1 );
        IndexDescriptor index = IndexPrototype.forSchema( schema ).withName( "index" ).materialise( 0 ).withIndexCapability( capability );
        IndexDescriptor completed = indexProvider.completeConfiguration( index );
        assertSame( capability, completed.getCapability() );
    }
}
