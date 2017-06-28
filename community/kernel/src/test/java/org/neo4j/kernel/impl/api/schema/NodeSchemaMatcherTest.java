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
package org.neo4j.kernel.impl.api.schema;

import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.neo4j.collection.primitive.Primitive;
import org.neo4j.collection.primitive.PrimitiveIntCollections;
import org.neo4j.collection.primitive.PrimitiveIntSet;
import org.neo4j.cursor.Cursor;
import org.neo4j.kernel.api.schema.index.IndexDescriptor;
import org.neo4j.kernel.impl.api.KernelStatement;
import org.neo4j.kernel.impl.api.operations.EntityReadOperations;
import org.neo4j.kernel.impl.api.state.StubCursors;
import org.neo4j.storageengine.api.NodeItem;
import org.neo4j.values.storable.Values;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.neo4j.helpers.collection.Iterators.iterator;
import static org.neo4j.kernel.api.schema.index.IndexDescriptorFactory.forLabel;

public class NodeSchemaMatcherTest
{

    private static final int labelId1 = 10;
    private static final int labelId2 = 11;
    private static final int nonExistentLabelId = 12;
    private static final int propId1 = 20;
    private static final int propId2 = 21;
    private static final int unIndexedPropId = 22;
    private static final int nonExistentPropId = 23;
    private static final int specialPropId = 24;

    KernelStatement state;
    NodeItem node;

    IndexDescriptor index1 = forLabel( labelId1, propId1 );
    IndexDescriptor index1_2 = forLabel( labelId1, propId1, propId2 );
    IndexDescriptor indexWithMissingProperty = forLabel( labelId1, propId1, nonExistentPropId );
    IndexDescriptor indexWithMissingLabel = forLabel( nonExistentLabelId, propId1, propId2 );
    IndexDescriptor indexOnSpecialProperty = forLabel( labelId1, propId1, specialPropId );

    private NodeSchemaMatcher nodeSchemaMatcher;

    @Before
    public void setup()
    {
        state = mock( KernelStatement.class );

        PrimitiveIntSet labels = Primitive.intSet();
        labels.add( labelId1 );

        Cursor<NodeItem> nodeItemCursor = StubCursors.asNodeCursor( 0, labels );
        nodeItemCursor.next();
        node = nodeItemCursor.get();

        PrimitiveIntSet defaultPropertyIds = PrimitiveIntCollections.asSet( new int[]{ propId1, propId2,
                unIndexedPropId} );
        EntityReadOperations readOps = mock( EntityReadOperations.class );
        when( readOps.nodeGetPropertyKeys( state, node ) ).thenReturn( defaultPropertyIds );
        when( readOps.nodeGetProperty( state, node, propId1 ) ).thenReturn( Values.of( "hi1" ) );
        when( readOps.nodeGetProperty( state, node, propId2 ) ).thenReturn( Values.of( "hi2" ) );
        when( readOps.nodeGetProperty( state, node, unIndexedPropId ) ).thenReturn( Values.of( "hi3" ) );

        nodeSchemaMatcher = new NodeSchemaMatcher( readOps );
    }

    @Test
    public void shouldMatchOnSingleProperty()
    {
        // when
        final List<IndexDescriptor> matched = new ArrayList<>();
        nodeSchemaMatcher.onMatchingSchema(
                state, iterator( index1 ), node, unIndexedPropId, ( schema, props ) -> matched.add( schema ) );

        // then
        assertThat( matched, contains( index1 ) );
    }

    @Test
    public void shouldMatchOnTwoProperties()
    {
        // when
        final List<IndexDescriptor> matched = new ArrayList<>();
        nodeSchemaMatcher.onMatchingSchema(
                state, iterator( index1_2 ), node, unIndexedPropId, ( schema, props ) -> matched.add( schema ) );

        // then
        assertThat( matched, contains( index1_2 ) );
    }

    @Test
    public void shouldNotMatchIfNodeIsMissingProperty()
    {
        // when
        final List<IndexDescriptor> matched = new ArrayList<>();
        nodeSchemaMatcher.onMatchingSchema( state, iterator( indexWithMissingProperty ), node, unIndexedPropId,
                ( schema, props ) -> matched.add( schema ) );

        // then
        assertThat( matched, empty() );
    }

    @Test
    public void shouldNotMatchIfNodeIsMissingLabel()
    {
        // when
        final List<IndexDescriptor> matched = new ArrayList<>();
        nodeSchemaMatcher.onMatchingSchema( state, iterator( indexWithMissingLabel ), node, unIndexedPropId,
                ( schema, props ) -> matched.add( schema ) );

        // then
        assertThat( matched, empty() );
    }

    @Test
    public void shouldMatchOnSpecialProperty()
    {
        // when
        final List<IndexDescriptor> matched = new ArrayList<>();
        nodeSchemaMatcher.onMatchingSchema( state, iterator( indexOnSpecialProperty ), node, specialPropId,
                ( schema, props ) -> matched.add( schema ) );

        // then
        assertThat( matched, contains( indexOnSpecialProperty ) );
    }

    @Test
    public void shouldMatchSeveralTimes()
    {
        // given
        List<IndexDescriptor> indexes = Arrays.asList( index1, index1, index1_2, index1_2 );

        // when
        final List<IndexDescriptor> matched = new ArrayList<>();
        nodeSchemaMatcher.onMatchingSchema(
                state, indexes.iterator(), node, unIndexedPropId, ( schema, props ) -> matched.add( schema ) );

        // then
        assertThat( matched, equalTo( indexes ) );
    }
}
