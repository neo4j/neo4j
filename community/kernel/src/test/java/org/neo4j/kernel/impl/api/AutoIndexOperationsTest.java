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
package org.neo4j.kernel.impl.api;

import org.junit.Before;
import org.junit.Test;

import org.neo4j.kernel.api.DataWriteOperations;
import org.neo4j.kernel.api.explicitindex.AutoIndexOperations;
import org.neo4j.kernel.impl.api.explicitindex.InternalAutoIndexOperations;
import org.neo4j.kernel.impl.core.PropertyKeyTokenHolder;
import org.neo4j.kernel.impl.core.TokenNotFoundException;
import org.neo4j.storageengine.api.Token;
import org.neo4j.values.storable.Values;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyZeroInteractions;
import static org.mockito.Mockito.when;
import static org.neo4j.kernel.impl.api.explicitindex.InternalAutoIndexing.NODE_AUTO_INDEX;

public class AutoIndexOperationsTest
{
    private final DataWriteOperations ops = mock(DataWriteOperations.class);
    private final PropertyKeyTokenHolder tokens = mock( PropertyKeyTokenHolder.class );
    private final AutoIndexOperations index = new InternalAutoIndexOperations( tokens, InternalAutoIndexOperations.EntityType.NODE );

    private final int nonIndexedProperty = 1337;
    private final String nonIndexedPropertyName = "foo";
    private final int indexedProperty = 1338;
    private final int indexedProperty2 = 1339;
    private final String indexedPropertyName = "bar";
    private final String indexedPropertyName2 = "baz";

    @Before
    public void setup() throws TokenNotFoundException
    {
        when(tokens.getTokenById( nonIndexedProperty )).thenReturn( new Token( nonIndexedPropertyName, nonIndexedProperty ) );
        when(tokens.getTokenById( indexedProperty )).thenReturn( new Token( indexedPropertyName, indexedProperty ) );
        when(tokens.getTokenById( indexedProperty2 )).thenReturn( new Token( indexedPropertyName, indexedProperty2 ) );
        index.enabled( true );
    }

    @Test
    public void shouldNotRemoveFromIndexForNonAutoIndexedProperty() throws Exception
    {
        // Given
        index.startAutoIndexingProperty( indexedPropertyName );

        // When
        index.propertyRemoved( ops, 11, nonIndexedProperty );

        // Then
        verifyZeroInteractions( ops );
    }

    @Test
    public void shouldRemoveSpecificValueFromIndexForAutoIndexedProperty() throws Exception
    {
        // Given
        long nodeId = 11;
        int value1 = 1;
        int value2 = 2;
        index.startAutoIndexingProperty( indexedPropertyName );
        index.startAutoIndexingProperty( indexedPropertyName2 );
        index.propertyAdded( ops, nodeId, indexedProperty, Values.of( value1 ) );
        index.propertyAdded( ops, nodeId, indexedProperty2, Values.of( value2 ) );

        // When
        reset( ops );
        index.propertyRemoved( ops, nodeId, indexedProperty );

        // Then
        verify( ops ).nodeRemoveFromExplicitIndex( NODE_AUTO_INDEX, nodeId, indexedPropertyName );
    }

    @Test
    public void shouldNotAddToIndexForNonAutoIndexedProperty() throws Exception
    {
        // Given
        index.startAutoIndexingProperty( indexedPropertyName );

        // When
        index.propertyAdded( ops, 11, nonIndexedProperty, Values.of( "Hello!" ) );

        // Then
        verifyZeroInteractions( ops );
    }

    @Test
    public void shouldNotAddOrRemoveFromIndexForNonAutoIndexedProperty() throws Exception
    {
        // Given
        index.startAutoIndexingProperty( indexedPropertyName );

        // When
        index.propertyChanged( ops, 11, nonIndexedProperty, Values.of( "Goodbye!" ), Values.of( "Hello!" ) );

        // Then
        verifyZeroInteractions( ops );
    }
}
