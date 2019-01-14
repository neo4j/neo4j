/*
 * Copyright (c) 2002-2019 "Neo4j,"
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
package org.neo4j.kernel.impl.newapi;


import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

import org.neo4j.internal.kernel.api.helpers.StubNodeCursor;
import org.neo4j.internal.kernel.api.helpers.StubPropertyCursor;
import org.neo4j.kernel.api.schema.index.SchemaIndexDescriptor;
import org.neo4j.values.storable.Value;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.neo4j.helpers.collection.Iterators.iterator;
import static org.neo4j.kernel.api.schema.index.SchemaIndexDescriptorFactory.forLabel;
import static org.neo4j.values.storable.Values.stringValue;

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

    SchemaIndexDescriptor index1 = forLabel( labelId1, propId1 );
    SchemaIndexDescriptor index1_2 = forLabel( labelId1, propId1, propId2 );
    SchemaIndexDescriptor indexWithMissingProperty = forLabel( labelId1, propId1, nonExistentPropId );
    SchemaIndexDescriptor indexWithMissingLabel = forLabel( nonExistentLabelId, propId1, propId2 );
    SchemaIndexDescriptor indexOnSpecialProperty = forLabel( labelId1, propId1, specialPropId );
    private StubNodeCursor node;

    @Before
    public void setup()
    {
        HashMap<Integer,Value> map = new HashMap<>();
        map.put( propId1, stringValue( "hello" ) );
        map.put( propId2, stringValue( "world" ) );
        map.put( unIndexedPropId, stringValue( "!" ) );
        node = new StubNodeCursor( false );
        node.withNode( 42, new long[]{labelId1}, map );
        node.next();
    }

    @Test
    public void shouldMatchOnSingleProperty()
    {
        // when
        List<SchemaIndexDescriptor> matched = new ArrayList<>();
        NodeSchemaMatcher.onMatchingSchema( iterator( index1 ), node,
                new StubPropertyCursor(), unIndexedPropId, ( schema, props ) -> matched.add( schema ) );

        // then
        assertThat( matched, contains( index1 ) );
    }

    @Test
    public void shouldMatchOnTwoProperties()
    {
        // when
        List<SchemaIndexDescriptor> matched = new ArrayList<>();
        NodeSchemaMatcher.onMatchingSchema( iterator( index1_2 ), node, new StubPropertyCursor(),
                unIndexedPropId, ( schema, props ) -> matched.add( schema ) );

        // then
        assertThat( matched, contains( index1_2 ) );
    }

    @Test
    public void shouldNotMatchIfNodeIsMissingProperty()
    {
        // when
        List<SchemaIndexDescriptor> matched = new ArrayList<>();
        NodeSchemaMatcher.onMatchingSchema( iterator( indexWithMissingProperty ), node, new StubPropertyCursor(),
                unIndexedPropId, ( schema, props ) -> matched.add( schema ) );

        // then
        assertThat( matched, empty() );
    }

    @Test
    public void shouldNotMatchIfNodeIsMissingLabel()
    {
        // when
        List<SchemaIndexDescriptor> matched = new ArrayList<>();
        NodeSchemaMatcher.onMatchingSchema( iterator( indexWithMissingLabel ), node, new StubPropertyCursor(),
                unIndexedPropId, ( schema, props ) -> matched.add( schema ) );

        // then
        assertThat( matched, empty() );
    }

    @Test
    public void shouldMatchOnSpecialProperty()
    {
        // when
        List<SchemaIndexDescriptor> matched = new ArrayList<>();
        NodeSchemaMatcher.onMatchingSchema( iterator( indexOnSpecialProperty ), node, new StubPropertyCursor(),
                specialPropId, ( schema, props ) -> matched.add( schema ) );

        // then
        assertThat( matched, contains( indexOnSpecialProperty ) );
    }

    @Test
    public void shouldMatchSeveralTimes()
    {
        // given
        List<SchemaIndexDescriptor> indexes = Arrays.asList( index1, index1, index1_2, index1_2 );

        // when
        final List<SchemaIndexDescriptor> matched = new ArrayList<>();
        NodeSchemaMatcher.onMatchingSchema(
                indexes.iterator(), node, new StubPropertyCursor(), unIndexedPropId,
                ( schema, props ) -> matched.add( schema ) );

        // then
        assertThat( matched, equalTo( indexes ) );
    }
}
