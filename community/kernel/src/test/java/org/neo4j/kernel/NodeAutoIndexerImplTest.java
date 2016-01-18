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
package org.neo4j.kernel;

import org.junit.Test;

import java.util.Collections;

import org.neo4j.graphdb.Node;
import org.neo4j.kernel.impl.coreapi.IndexProvider;
import org.neo4j.kernel.impl.coreapi.NodeAutoIndexerImpl;
import org.neo4j.kernel.impl.factory.GraphDatabaseFacade;

import static org.mockito.Mockito.RETURNS_MOCKS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verifyZeroInteractions;

public class NodeAutoIndexerImplTest
{

    private final IndexProvider indexProvider = mock( IndexProvider.class, RETURNS_MOCKS );
    private final NodeAutoIndexerImpl index = new NodeAutoIndexerImpl( true, Collections.emptyList(), indexProvider, mock( GraphDatabaseFacade.SPI.class ) );

    @Test
    public void shouldNotRemoveFromIndexForNonAutoIndexedProperty() throws Exception
    {
        // Given
        String indexedPropertyName = "someProperty";
        String nonIndexedPropertyName = "someOtherProperty";
        index.startAutoIndexingProperty( indexedPropertyName );

        // When
        index.propertyRemoved( mock( Node.class ), nonIndexedPropertyName, new Object() );

        // Then
        verifyZeroInteractions( indexProvider );
    }

    @Test
    public void shouldNotAddToIndexForNonAutoIndexedProperty() throws Exception
    {
        // Given
        String indexedPropertyName = "someProperty";
        String nonIndexedPropertyName = "someOtherProperty";
        index.startAutoIndexingProperty( indexedPropertyName );

        // When
        index.propertyAdded( mock( Node.class ), nonIndexedPropertyName, new Object() );

        // Then
        verifyZeroInteractions( indexProvider );
    }

    @Test
    public void shouldNotAddOrRemoveFromIndexForNonAutoIndexedProperty() throws Exception
    {
        // Given
        String indexedPropertyName = "someProperty";
        String nonIndexedPropertyName = "someOtherProperty";
        index.startAutoIndexingProperty( indexedPropertyName );

        // When
        index.propertyChanged( mock( Node.class ), nonIndexedPropertyName, new Object(), new Object() );

        // Then
        verifyZeroInteractions( indexProvider );
    }
}
