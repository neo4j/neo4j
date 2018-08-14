/*
 * Copyright (c) 2002-2018 "Neo4j,"
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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.neo4j.internal.kernel.api.IndexQuery;
import org.neo4j.kernel.api.schema.index.TestIndexDescriptorFactory;
import org.neo4j.storageengine.api.schema.IndexDescriptor;
import org.neo4j.values.storable.RandomValues;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.Values;

import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

@Ignore( "Not a test. This is a compatibility suite that provides test cases for verifying" +
        " IndexProvider implementations. Each index provider that is to be tested by this suite" +
        " must create their own test class extending IndexProviderCompatibilityTestSuite." +
        " The @Ignore annotation doesn't prevent these tests to run, it rather removes some annoying" +
        " errors or warnings in some IDEs about test classes needing a public zero-arg constructor." )
public abstract class SimpleRandomizedIndexAccessorCompatibility extends IndexAccessorCompatibility
{
    public SimpleRandomizedIndexAccessorCompatibility( IndexProviderCompatibilityTestSuite testSuite, IndexDescriptor descriptor )
    {
        super( testSuite, descriptor );
    }

    @Test
    public void testRandomValues() throws Exception
    {
        Map<Value,List<IndexEntryUpdate<?>>> updates = createRandomUpdates( 10000 );
        for ( Value value : updates.keySet() )
        {
            updateAndCommit( updates.get( value ) );
        }

        for ( Value value : updates.keySet() )
        {
            List<Long> expectedEntityIds = updates.get( value ).stream()
                    .map( IndexEntryUpdate::getEntityId )
                    .collect( Collectors.toList() );
            IndexQuery.ExactPredicate exact = IndexQuery.exact( 100, value );
            List<Long> result = query( exact );
            if ( isGeometryValue( value ) )
            {
                // Because spatial index can have false positives we can only assert on contains
                for ( Long expectedEntityId : expectedEntityIds )
                {
                    assertTrue( "query: " + exact, result.contains( expectedEntityId ) );
                }
            }
            else
            {
                assertThat( "query: " + exact, result, equalTo( expectedEntityIds ) );
            }
        }
    }

    @SuppressWarnings( "SameParameterValue" )
    private Map<Value,List<IndexEntryUpdate<?>>> createRandomUpdates( int numberOfUpdates )
    {
        // Generate what types to include
        List<RandomValues.Types> validValueTypes = listOfRandomValidValueTypes();

        Map<Value,List<IndexEntryUpdate<?>>> map = new HashMap<>();
        for ( int i = 0; i < numberOfUpdates; i++ )
        {
            Value value;
            do
            {
                value = randomValueFromValidTypes( validValueTypes );
            }
            while ( isGeometryValue( value ) && !testSuite.supportsSpatial() );
            map.computeIfAbsent( value, v -> new ArrayList<>() )
                    .add( IndexEntryUpdate.add( i, descriptor.schema(), value ) );
        }
        return map;
    }

    private List<RandomValues.Types> listOfRandomValidValueTypes()
    {
        List<RandomValues.Types> validValueTypes = new ArrayList<>( Arrays.asList( RandomValues.Types.values() ) );
        if ( !testSuite.supportsSpatial() )
        {
            removeSpatialTypes( validValueTypes );
        }
        Collections.shuffle( validValueTypes );
        int numberOfGroupsToUse = random.nextInt( 1, validValueTypes.size() - 1 );
        while ( validValueTypes.size() > numberOfGroupsToUse )
        {
            validValueTypes.remove( validValueTypes.size() - 1 );
        }
        return validValueTypes;
    }

    private void removeSpatialTypes( List<RandomValues.Types> targetValueTypes )
    {
        targetValueTypes.remove( RandomValues.Types.CARTESIAN_POINT );
        targetValueTypes.remove( RandomValues.Types.CARTESIAN_POINT_3D );
        targetValueTypes.remove( RandomValues.Types.GEOGRAPHIC_POINT );
        targetValueTypes.remove( RandomValues.Types.GEOGRAPHIC_POINT_3D );
    }

    private Value randomValueFromValidTypes( List<RandomValues.Types> validValueTypes )
    {
        int targetValueType = random.nextInt( validValueTypes.size() );
        return random.nextValue( validValueTypes.get( targetValueType ) );
    }

    private boolean isGeometryValue( Value value )
    {
        return Values.isGeometryValue( value ) || Values.isGeometryArray( value );
    }

    @Ignore( "Not a test. This is a compatibility suite" )
    public static class General extends SimpleRandomizedIndexAccessorCompatibility
    {
        public General( IndexProviderCompatibilityTestSuite testSuite )
        {
            super( testSuite, TestIndexDescriptorFactory.forLabel( 1000, 100 ) );
        }
    }
}
