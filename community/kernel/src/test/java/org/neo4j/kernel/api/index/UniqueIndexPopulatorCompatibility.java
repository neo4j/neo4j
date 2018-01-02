/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.neo4j.kernel.api.properties.Property.stringProperty;

import org.junit.Ignore;
import org.junit.Test;

import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.api.index.sampling.IndexSamplingConfig;

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

        IndexConfiguration indexConfig = new IndexConfiguration( true );
        IndexSamplingConfig indexSamplingConfig = new IndexSamplingConfig( new Config() );
        IndexPopulator populator = indexProvider.getPopulator( 17, descriptor, indexConfig, indexSamplingConfig );
        populator.create();
        populator.add( nodeId1, value );
        populator.add( nodeId2, value );
        try
        {
            PropertyAccessor propertyAccessor = mock( PropertyAccessor.class );
            int propertyKeyId = descriptor.getPropertyKeyId();
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
