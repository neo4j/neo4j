/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
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
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */
package org.neo4j.kernel.impl.newapi;

import static org.assertj.core.api.Assertions.assertThat;
import static org.neo4j.internal.helpers.collection.Iterators.iterator;
import static org.neo4j.kernel.api.schema.index.TestIndexDescriptorFactory.forLabel;
import static org.neo4j.kernel.api.schema.index.TestIndexDescriptorFactory.forRelType;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.neo4j.internal.schema.IndexDescriptor;

abstract class SchemaMatcherTest {
    private static final int tokenId1 = 10;
    private static final int nonExistentTokenId = 12;
    private static final int propId1 = 20;
    private static final int propId2 = 21;
    private static final int unIndexedPropId = 22;
    private static final int nonExistentPropId = 23;
    private static final int specialPropId = 24;
    private static final int[] props = {propId1, propId2, unIndexedPropId};

    private final IndexDescriptor index1 = index1();
    private final IndexDescriptor index1_2 = index1_2();
    private final IndexDescriptor indexWithMissingProperty = indexWithMissingProperty();
    private final IndexDescriptor indexWithMissingToken = indexWithMissingToken();
    private final IndexDescriptor indexOnSpecialProperty = indexOnSpecialProperty();

    @Test
    void shouldMatchOnSingleProperty() {
        // when
        List<IndexDescriptor> matched = new ArrayList<>();
        SchemaMatcher.onMatchingSchema(iterator(index1), unIndexedPropId, props, matched::add);

        // then
        assertThat(matched).containsExactly(index1);
    }

    @Test
    void shouldMatchOnTwoProperties() {
        // when
        List<IndexDescriptor> matched = new ArrayList<>();
        SchemaMatcher.onMatchingSchema(iterator(index1_2), unIndexedPropId, props, matched::add);

        // then
        assertThat(matched).containsExactly(index1_2);
    }

    @Test
    void shouldNotMatchIfEntityIsMissingProperty() {
        // when
        List<IndexDescriptor> matched = new ArrayList<>();
        SchemaMatcher.onMatchingSchema(iterator(indexWithMissingProperty), unIndexedPropId, props, matched::add);

        // then
        assertThat(matched).isEmpty();
    }

    @Test
    void shouldMatchOnSpecialProperty() {
        // when
        List<IndexDescriptor> matched = new ArrayList<>();
        SchemaMatcher.onMatchingSchema(iterator(indexOnSpecialProperty), specialPropId, props, matched::add);

        // then
        assertThat(matched).containsExactly(indexOnSpecialProperty);
    }

    @Test
    void shouldMatchSeveralTimes() {
        // given
        List<IndexDescriptor> indexes = Arrays.asList(index1, index1, index1_2, index1_2);

        // when
        final List<IndexDescriptor> matched = new ArrayList<>();
        SchemaMatcher.onMatchingSchema(indexes.iterator(), unIndexedPropId, props, matched::add);

        // then
        assertThat(matched).isEqualTo(indexes);
    }

    abstract IndexDescriptor index1();

    abstract IndexDescriptor index1_2();

    abstract IndexDescriptor indexWithMissingProperty();

    abstract IndexDescriptor indexWithMissingToken();

    abstract IndexDescriptor indexOnSpecialProperty();

    static class ForNode extends SchemaMatcherTest {

        @Override
        IndexDescriptor index1() {
            return forLabel(tokenId1, propId1);
        }

        @Override
        IndexDescriptor index1_2() {
            return forLabel(tokenId1, propId1, propId2);
        }

        @Override
        IndexDescriptor indexWithMissingProperty() {
            return forLabel(tokenId1, propId1, nonExistentPropId);
        }

        @Override
        IndexDescriptor indexWithMissingToken() {
            return forLabel(nonExistentTokenId, propId1, propId2);
        }

        @Override
        IndexDescriptor indexOnSpecialProperty() {
            return forLabel(tokenId1, propId1, specialPropId);
        }
    }

    static class ForRelationship extends SchemaMatcherTest {

        @Override
        IndexDescriptor index1() {
            return forRelType(tokenId1, propId1);
        }

        @Override
        IndexDescriptor index1_2() {
            return forRelType(tokenId1, propId1, propId2);
        }

        @Override
        IndexDescriptor indexWithMissingProperty() {
            return forRelType(tokenId1, propId1, nonExistentPropId);
        }

        @Override
        IndexDescriptor indexWithMissingToken() {
            return forRelType(nonExistentTokenId, propId1, propId2);
        }

        @Override
        IndexDescriptor indexOnSpecialProperty() {
            return forRelType(tokenId1, propId1, specialPropId);
        }
    }
}
