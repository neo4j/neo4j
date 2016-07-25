/*
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
package org.neo4j.kernel.impl.api;

import org.junit.Before;
import org.junit.Test;

import org.neo4j.kernel.api.DataWriteOperations;
import org.neo4j.kernel.api.legacyindex.AutoIndexOperations;
import org.neo4j.kernel.impl.api.legacyindex.InternalAutoIndexOperations;
import org.neo4j.kernel.impl.core.PropertyKeyTokenHolder;
import org.neo4j.kernel.impl.core.TokenNotFoundException;
import org.neo4j.storageengine.api.Token;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verifyZeroInteractions;
import static org.mockito.Mockito.when;
import static org.neo4j.kernel.api.properties.Property.property;

public class AutoIndexOperationsTest
{
    private final DataWriteOperations ops = mock(DataWriteOperations.class);
    private final PropertyKeyTokenHolder tokens = mock( PropertyKeyTokenHolder.class );
    private final AutoIndexOperations index = new InternalAutoIndexOperations( tokens, InternalAutoIndexOperations.EntityType.NODE );

    private final int nonIndexedProperty = 1337;
    private final String nonIndexedPropertyName = "foo";
    private final int indexedProperty = 1338;
    private final String indexedPropertyName = "bar";

    @Before
    public void setup() throws TokenNotFoundException
    {
        when(tokens.getTokenById( nonIndexedProperty )).thenReturn( new Token( nonIndexedPropertyName, 1337 ) );
        when(tokens.getTokenById( indexedProperty )).thenReturn( new Token( indexedPropertyName, 1337 ) );
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
    public void shouldNotAddToIndexForNonAutoIndexedProperty() throws Exception
    {
        // Given
        index.startAutoIndexingProperty( indexedPropertyName );

        // When
        index.propertyAdded( ops, 11, property( nonIndexedProperty, "Hello!" ) );

        // Then
        verifyZeroInteractions( ops );
    }

    @Test
    public void shouldNotAddOrRemoveFromIndexForNonAutoIndexedProperty() throws Exception
    {
        // Given
        index.startAutoIndexingProperty( indexedPropertyName );

        // When
        index.propertyChanged( ops, 11, property( nonIndexedProperty, "Goodbye!" ), property( nonIndexedProperty, "Hello!" ) );

        // Then
        verifyZeroInteractions( ops );
    }
}
