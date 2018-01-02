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
package org.neo4j.kernel.impl.api.store;

import org.junit.Test;

import java.util.Set;

import org.neo4j.kernel.api.constraints.NodePropertyConstraint;
import org.neo4j.kernel.api.constraints.PropertyConstraint;
import org.neo4j.kernel.api.constraints.UniquenessConstraint;
import org.neo4j.kernel.api.index.IndexDescriptor;
import org.neo4j.kernel.impl.api.KernelStatement;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertFalse;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verifyZeroInteractions;
import static org.mockito.Mockito.when;
import static org.neo4j.helpers.collection.IteratorUtil.asSet;

public class CacheLayerTest
{
    private final DiskLayer diskLayer = mock( DiskLayer.class );
    private final SchemaCache schemaCache = mock( SchemaCache.class );
    private final CacheLayer context = new CacheLayer( diskLayer, schemaCache, new ProcedureCache() );

    @Test
    public void shouldLoadAllConstraintsFromCache() throws Exception
    {
        // Given
        Set<PropertyConstraint> constraints = asSet( (PropertyConstraint) new UniquenessConstraint( 0, 1 ) );
        when( schemaCache.constraints() ).thenReturn( constraints.iterator() );

        // When & Then
        assertThat( asSet( context.constraintsGetAll() ), equalTo( constraints ) );
    }

    @Test
    public void shouldLoadConstraintsByLabelFromCache() throws Exception
    {
        // Given
        int labelId = 0;
        Set<NodePropertyConstraint> constraints = asSet( (NodePropertyConstraint) new UniquenessConstraint( labelId, 1 ) );
        when( schemaCache.constraintsForLabel( labelId ) ).thenReturn( constraints.iterator() );

        // When & Then
        assertThat( asSet( context.constraintsGetForLabel( labelId ) ), equalTo( constraints ) );
    }

    @Test
    public void shouldLoadConstraintsByLabelAndPropertyFromCache() throws Exception
    {
        // Given
        int labelId = 0, propertyId = 1;
        Set<NodePropertyConstraint> constraints = asSet( (NodePropertyConstraint) new UniquenessConstraint( labelId, propertyId ) );
        when( schemaCache.constraintsForLabelAndProperty( labelId, propertyId ) ).thenReturn( constraints.iterator() );

        // When & Then
        assertThat( asSet( context.constraintsGetForLabelAndPropertyKey( labelId, propertyId ) ), equalTo( constraints ) );
    }

    @Test
    public void shouldNotCallToDiskLayerOnEmptyRangeSeekByString() throws Exception
    {
        // Given
        int labelId = 0, propertyId = 1;
        IndexDescriptor index = new IndexDescriptor( labelId, propertyId );
        KernelStatement statement = mock( KernelStatement.class );

        // When
        assertFalse(
            context.nodesGetFromIndexRangeSeekByString( statement, index, "b", false, "a", false ).hasNext()
        );

        // Then
        verifyZeroInteractions( schemaCache );
        verifyZeroInteractions( statement );
        verifyZeroInteractions( diskLayer );
    }
}
