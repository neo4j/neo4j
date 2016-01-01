/**
 * Copyright (c) 2002-2016 "Neo Technology,"
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
package org.neo4j.kernel.impl.core;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import org.junit.Test;
import org.junit.experimental.theories.DataPoint;
import org.junit.experimental.theories.Theories;
import org.junit.experimental.theories.Theory;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;

import org.neo4j.kernel.api.properties.DefinedProperty;
import org.neo4j.kernel.api.properties.Property;
import org.neo4j.kernel.impl.api.store.CacheLoader;
import org.neo4j.kernel.impl.api.store.CacheUpdateListener;

import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith( Theories.class )
public class PrimitivePropertyLoadingTest
{
    private static final long entityId = 42;

    @DataPoint
    public static Primitive nodeImpl = new NodeImpl( entityId );
    @DataPoint
    public static Primitive relationshipImpl = new RelationshipImpl( entityId, 2, 3, 4, false );

    @Test
    public void shouldSortTheGraphPropertyChainPriorToVerifyingIt() throws Exception
    {
        GraphPropertiesImpl graphProperties = new GraphPropertiesImpl( null, null );
        shouldSortThePropertyChainPriorToVerifyingIt( graphProperties );
    }

    @Theory
    @Test
    public void shouldSortThePropertyChainPriorToVerifyingIt( Primitive dataPoint ) throws Exception
    {
        DefinedProperty property2 = Property.booleanProperty( 2, true );
        DefinedProperty property1 = Property.booleanProperty( 1, false );
        DefinedProperty property3 = Property.booleanProperty( 3, true );
        List<DefinedProperty> values = Arrays.asList(
                property2,
                property1,
                property3
        );

        @SuppressWarnings( "unchecked" )
        CacheLoader<Iterator<DefinedProperty>> loader = mock( CacheLoader.class );
        when( loader.load( entityId ) ).thenReturn( values.iterator() );
        when( loader.load( -1 ) ).thenReturn( values.iterator() ); // for the GraphPropertiesImpl

        CacheUpdateListener updateListener = CacheUpdateListener.NO_UPDATES;
        PropertyChainVerifier chainVerifier = mock( PropertyChainVerifier.class );
        ArgumentCaptor<DefinedProperty[]> verifiedProperties = ArgumentCaptor.forClass( DefinedProperty[].class );
        ArgumentCaptor<Primitive> verifiedEntity = ArgumentCaptor.forClass( Primitive.class );

        dataPoint.getProperties( loader, updateListener, chainVerifier );

        verify( chainVerifier ).verifySortedPropertyChain(
                verifiedProperties.capture(), verifiedEntity.capture() );
        DefinedProperty[] verifiedPropertiesValue = verifiedProperties.getValue();
        assertTrue( Arrays.equals(
                verifiedPropertiesValue,
                new DefinedProperty[]{ property1, property2, property3 } ) );
    }
}
