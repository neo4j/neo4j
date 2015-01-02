/**
 * Copyright (c) 2002-2015 "Neo Technology,"
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
package org.neo4j.kernel.impl.api.store;

import java.util.Set;

import org.junit.Test;

import org.neo4j.kernel.impl.api.KernelStatement;
import org.neo4j.kernel.api.constraints.UniquenessConstraint;
import org.neo4j.kernel.api.exceptions.EntityNotFoundException;
import org.neo4j.kernel.impl.api.index.IndexingService;
import org.neo4j.kernel.impl.util.PrimitiveIntIterator;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertArrayEquals;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import static org.neo4j.helpers.collection.IteratorUtil.asSet;
import static org.neo4j.kernel.impl.api.StatementOperationsTestHelper.mockedState;
import static org.neo4j.kernel.impl.util.PrimitiveIntIteratorForArray.primitiveIntIteratorToIntArray;

public class CacheLayerTest
{
    private final DiskLayer diskLayer = mock( DiskLayer.class );
    private final PersistenceCache persistenceCache = mock( PersistenceCache.class );
    private final SchemaCache schemaCache = mock( SchemaCache.class );
    private final IndexingService indexingService = mock( IndexingService.class );
    private final CacheLayer context = new CacheLayer( diskLayer, persistenceCache, indexingService, schemaCache );

    @Test
    public void shouldGetCachedLabelsIfCached() throws EntityNotFoundException
    {
        // GIVEN
        long nodeId = 3;
        int[] labels = new int[] {1, 2, 3};
        when( persistenceCache.nodeGetLabels( any( KernelStatement.class ), eq( nodeId ), any( CacheLoader.class ) ) )
                .thenReturn( labels );

        // WHEN
        PrimitiveIntIterator receivedLabels = context.nodeGetLabels( mockedState(), nodeId );

        // THEN
        assertArrayEquals( labels, primitiveIntIteratorToIntArray( receivedLabels ) );
    }

    @Test
    public void shouldLoadAllConstraintsFromCache() throws Exception
    {
        // Given
        Set<UniquenessConstraint> constraints = asSet( new UniquenessConstraint( 0, 1 ) );
        when(schemaCache.constraints()).thenReturn( constraints.iterator() );

        // When & Then
        assertThat( asSet( context.constraintsGetAll( mockedState() ) ), equalTo( constraints ) );
    }

    @Test
    public void shouldLoadConstraintsByLabelFromCache() throws Exception
    {
        // Given
        int labelId =  0;
        Set<UniquenessConstraint> constraints = asSet( new UniquenessConstraint( labelId, 1 ) );
        when(schemaCache.constraintsForLabel(labelId)).thenReturn( constraints.iterator() );

        // When & Then
        assertThat( asSet( context.constraintsGetForLabel( mockedState(), labelId ) ), equalTo( constraints ) );
    }

    @Test
    public void shouldLoadConstraintsByLabelAndPropertyFromCache() throws Exception
    {
        // Given
        int labelId = 0, propertyId = 1;
        Set<UniquenessConstraint> constraints = asSet( new UniquenessConstraint( labelId, propertyId ) );
        when(schemaCache.constraintsForLabelAndProperty(labelId, propertyId)).thenReturn( constraints.iterator() );

        // When & Then
        assertThat( asSet( context.constraintsGetForLabelAndPropertyKey( mockedState(), labelId, propertyId ) ),
                equalTo( constraints ) );
    }
}
