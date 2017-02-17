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
package org.neo4j.kernel.api.index;

import org.junit.Ignore;
import org.junit.Test;

import java.util.Arrays;

import org.neo4j.kernel.api.schema_new.index.IndexBoundary;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.api.index.sampling.IndexSamplingConfig;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.neo4j.kernel.api.properties.Property.stringProperty;

@Ignore( "Not a test. This is a compatibility suite that provides test cases for verifying" +
        " SchemaIndexProvider implementations. Each index provider that is to be tested by this suite" +
        " must create their own test class extending IndexProviderCompatibilityTestSuite." +
        " The @Ignore annotation doesn't prevent these tests to run, it rather removes some annoying" +
        " errors or warnings in some IDEs about test classes needing a public zero-arg constructor." )
public class UniqueIndexPopulatorCompatibility extends IndexProviderCompatibilityTestSuite.Compatibility
{
    public UniqueIndexPopulatorCompatibility( IndexProviderCompatibilityTestSuite testSuite )
    {
        super( testSuite );
    }

    /**
     * This is also checked by the UniqueConstraintCompatibility test, only not on this abstraction level.
     */
    @Test
    public void shouldProvidePopulatorThatEnforcesUniqueConstraints() throws Exception
    {
        // when
        String value = "value1";
        int nodeId1 = 1;
        int nodeId2 = 2;

        IndexConfiguration indexConfig = IndexConfiguration.UNIQUE;
        IndexSamplingConfig indexSamplingConfig = new IndexSamplingConfig( Config.empty() );
        IndexPopulator populator =
                indexProvider.getPopulator( 17, IndexBoundary.map( descriptor ), indexConfig, indexSamplingConfig );
        populator.create();
        populator.add( Arrays.asList( IndexEntryUpdate.add( nodeId1, descriptor, value ),
                IndexEntryUpdate.add( nodeId2, descriptor, value ) ) );
        try
        {
            PropertyAccessor propertyAccessor = mock( PropertyAccessor.class );
            int propertyKeyId = descriptor.schema().getPropertyId();
            when( propertyAccessor.getProperty( nodeId1, propertyKeyId )).thenReturn(
                    stringProperty( propertyKeyId, value ) );
            when( propertyAccessor.getProperty( nodeId2, propertyKeyId )).thenReturn(
                    stringProperty( propertyKeyId, value ) );

            populator.verifyDeferredConstraints( propertyAccessor );

            fail( "expected exception" );
        }
        // then
        catch ( PreexistingIndexEntryConflictException conflict )
        {
            assertEquals( nodeId1, conflict.getExistingNodeId() );
            assertEquals( value, conflict.getPropertyValue() );
            assertEquals( nodeId2, conflict.getAddedNodeId() );
        }
    }
}
