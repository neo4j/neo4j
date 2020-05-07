/*
 * Copyright (c) 2002-2020 "Neo4j,"
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

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.neo4j.internal.kernel.api.helpers.StubNodeCursor;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.values.storable.Value;

import static org.assertj.core.api.Assertions.assertThat;
import static org.neo4j.internal.helpers.collection.Iterators.iterator;
import static org.neo4j.kernel.api.schema.index.TestIndexDescriptorFactory.forLabel;
import static org.neo4j.values.storable.Values.stringValue;

class NodeSchemaMatcherTest
{
    private static final int labelId1 = 10;
    private static final int nonExistentLabelId = 12;
    private static final int propId1 = 20;
    private static final int propId2 = 21;
    private static final int unIndexedPropId = 22;
    private static final int nonExistentPropId = 23;
    private static final int specialPropId = 24;
    private static final int[] props = {propId1, propId2, unIndexedPropId};

    private final IndexDescriptor index1 = forLabel( labelId1, propId1 );
    private final IndexDescriptor index1_2 = forLabel( labelId1, propId1, propId2 );
    private final IndexDescriptor indexWithMissingProperty = forLabel( labelId1, propId1, nonExistentPropId );
    private final IndexDescriptor indexWithMissingLabel = forLabel( nonExistentLabelId, propId1, propId2 );
    private final IndexDescriptor indexOnSpecialProperty = forLabel( labelId1, propId1, specialPropId );
    private StubNodeCursor node;

    @BeforeEach
    void setup()
    {
        Map<Integer,Value> map = new HashMap<>();
        map.put( propId1, stringValue( "hello" ) );
        map.put( propId2, stringValue( "world" ) );
        map.put( unIndexedPropId, stringValue( "!" ) );
        node = new StubNodeCursor( false );
        node.withNode( 42, new long[]{labelId1}, map );
        node.next();
    }

    @Test
    void shouldMatchOnSingleProperty()
    {
        // when
        List<IndexDescriptor> matched = new ArrayList<>();
        NodeSchemaMatcher.onMatchingSchema( iterator( index1 ), unIndexedPropId, props, matched::add );

        // then
        assertThat( matched ).containsExactly( index1 );
    }

    @Test
    void shouldMatchOnTwoProperties()
    {
        // when
        List<IndexDescriptor> matched = new ArrayList<>();
        NodeSchemaMatcher.onMatchingSchema( iterator( index1_2 ), unIndexedPropId, props, matched::add );

        // then
        assertThat( matched ).containsExactly( index1_2 );
    }

    @Test
    void shouldNotMatchIfNodeIsMissingProperty()
    {
        // when
        List<IndexDescriptor> matched = new ArrayList<>();
        NodeSchemaMatcher.onMatchingSchema( iterator( indexWithMissingProperty ), unIndexedPropId, props, matched::add );

        // then
        assertThat( matched ).isEmpty();
    }

    @Test
    void shouldNotMatchIfNodeIsMissingLabel()
    {
        // when
        List<IndexDescriptor> matched = new ArrayList<>();
        NodeSchemaMatcher.onMatchingSchema( iterator( indexWithMissingLabel ), node.labels().all(), unIndexedPropId, props, matched::add );

        // then
        assertThat( matched ).isEmpty();
    }

    @Test
    void shouldMatchOnSpecialProperty()
    {
        // when
        List<IndexDescriptor> matched = new ArrayList<>();
        NodeSchemaMatcher.onMatchingSchema( iterator( indexOnSpecialProperty ), specialPropId, props, matched::add );

        // then
        assertThat( matched ).containsExactly( indexOnSpecialProperty );
    }

    @Test
    void shouldMatchSeveralTimes()
    {
        // given
        List<IndexDescriptor> indexes = Arrays.asList( index1, index1, index1_2, index1_2 );

        // when
        final List<IndexDescriptor> matched = new ArrayList<>();
        NodeSchemaMatcher.onMatchingSchema( indexes.iterator(), unIndexedPropId, props, matched::add );

        // then
        assertThat( matched ).isEqualTo( indexes );
    }
}
