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
package org.neo4j.kernel.impl.transaction.state;

import static java.util.Arrays.asList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.neo4j.collection.Dependencies.dependenciesOf;

import java.util.ArrayList;
import org.junit.jupiter.api.Test;
import org.neo4j.collection.Dependencies;
import org.neo4j.internal.schema.IndexProviderDescriptor;
import org.neo4j.internal.schema.IndexType;
import org.neo4j.kernel.api.impl.fulltext.FulltextIndexProvider;
import org.neo4j.kernel.api.impl.schema.TextIndexProvider;
import org.neo4j.kernel.api.impl.schema.trigram.TrigramIndexProvider;
import org.neo4j.kernel.api.impl.schema.vector.VectorIndexProvider;
import org.neo4j.kernel.api.index.IndexProvider;
import org.neo4j.kernel.impl.api.index.IndexProviderNotFoundException;
import org.neo4j.kernel.impl.index.schema.PointIndexProvider;
import org.neo4j.kernel.impl.index.schema.RangeIndexProvider;
import org.neo4j.kernel.impl.index.schema.TokenIndexProvider;

class StaticIndexProviderMapTest {

    @Test
    void testGetters() throws Exception {
        var tokenIndexProvider = mockProvider(TokenIndexProvider.class, IndexType.LOOKUP);
        var rangeIndexProvider = mockProvider(RangeIndexProvider.class, IndexType.RANGE);
        var pointIndexProvider = mockProvider(PointIndexProvider.class, IndexType.POINT);
        var textIndexProvider = mockProvider(TextIndexProvider.class, IndexType.TEXT);
        var trigramIndexProvider = mockProvider(TrigramIndexProvider.class, IndexType.TEXT);
        var fulltextIndexProvider = mockProvider(FulltextIndexProvider.class, IndexType.FULLTEXT);
        var vectorIndexProvider = mockProvider(VectorIndexProvider.class, IndexType.VECTOR);
        var map = new StaticIndexProviderMap(
                tokenIndexProvider,
                rangeIndexProvider,
                pointIndexProvider,
                textIndexProvider,
                trigramIndexProvider,
                fulltextIndexProvider,
                vectorIndexProvider,
                new Dependencies());
        map.init();

        assertThat(map.getTokenIndexProvider()).isEqualTo(tokenIndexProvider);
        assertThat(map.getDefaultProvider()).isEqualTo(rangeIndexProvider);
        assertThat(map.getTextIndexProvider()).isEqualTo(trigramIndexProvider);
        assertThat(map.getFulltextProvider()).isEqualTo(fulltextIndexProvider);
        assertThat(map.getPointIndexProvider()).isEqualTo(pointIndexProvider);
        assertThat(map.getVectorIndexProvider()).isEqualTo(vectorIndexProvider);
    }

    @Test
    void testLookup() throws Exception {
        var tokenIndexProvider = mockProvider(TokenIndexProvider.class, IndexType.LOOKUP);
        var rangeIndexProvider = mockProvider(RangeIndexProvider.class, IndexType.RANGE);
        var pointIndexProvider = mockProvider(PointIndexProvider.class, IndexType.POINT);
        var textIndexProvider = mockProvider(TextIndexProvider.class, IndexType.TEXT);
        var trigramIndexProvider = mockProvider(TrigramIndexProvider.class, IndexType.TEXT);
        var fulltextIndexProvider = mockProvider(FulltextIndexProvider.class, IndexType.FULLTEXT);
        var vectorIndexProvider = mockProvider(VectorIndexProvider.class, IndexType.VECTOR);
        var map = new StaticIndexProviderMap(
                tokenIndexProvider,
                rangeIndexProvider,
                pointIndexProvider,
                textIndexProvider,
                trigramIndexProvider,
                fulltextIndexProvider,
                vectorIndexProvider,
                new Dependencies());
        map.init();

        asList(
                        tokenIndexProvider,
                        rangeIndexProvider,
                        pointIndexProvider,
                        textIndexProvider,
                        trigramIndexProvider,
                        fulltextIndexProvider,
                        vectorIndexProvider)
                .forEach(p -> {
                    assertThat(map.lookup(p.getProviderDescriptor()))
                            .as("lookup by descriptor")
                            .isEqualTo(p);

                    assertThat(map.lookup(p.getProviderDescriptor().name()))
                            .as("lookup by descriptor name")
                            .isEqualTo(p);
                });
    }

    @Test
    void testAccept() throws Exception {
        var tokenIndexProvider = mockProvider(TokenIndexProvider.class, IndexType.LOOKUP);
        var rangeIndexProvider = mockProvider(RangeIndexProvider.class, IndexType.RANGE);
        var pointIndexProvider = mockProvider(PointIndexProvider.class, IndexType.POINT);
        var textIndexProvider = mockProvider(TextIndexProvider.class, IndexType.TEXT);
        var trigramIndexProvider = mockProvider(TrigramIndexProvider.class, IndexType.TEXT);
        var fulltextIndexProvider = mockProvider(FulltextIndexProvider.class, IndexType.FULLTEXT);
        var vectorIndexProvider = mockProvider(VectorIndexProvider.class, IndexType.VECTOR);
        var map = new StaticIndexProviderMap(
                tokenIndexProvider,
                rangeIndexProvider,
                pointIndexProvider,
                textIndexProvider,
                trigramIndexProvider,
                fulltextIndexProvider,
                vectorIndexProvider,
                new Dependencies());
        map.init();

        var accepted = new ArrayList<>();
        map.accept(accepted::add);

        assertThat(accepted)
                .containsExactlyInAnyOrder(
                        tokenIndexProvider,
                        rangeIndexProvider,
                        textIndexProvider,
                        trigramIndexProvider,
                        fulltextIndexProvider,
                        pointIndexProvider,
                        vectorIndexProvider);
    }

    @Test
    void testWithExtension() throws Exception {
        var extension = mockProvider(IndexProvider.class, IndexType.RANGE);
        RangeIndexProvider rangeIndexProvider = mockProvider(RangeIndexProvider.class, IndexType.RANGE);
        var map = new StaticIndexProviderMap(
                mockProvider(TokenIndexProvider.class, IndexType.LOOKUP),
                rangeIndexProvider,
                mockProvider(PointIndexProvider.class, IndexType.POINT),
                mockProvider(TextIndexProvider.class, IndexType.TEXT),
                mockProvider(TrigramIndexProvider.class, IndexType.TEXT),
                mockProvider(FulltextIndexProvider.class, IndexType.FULLTEXT),
                mockProvider(VectorIndexProvider.class, IndexType.VECTOR),
                dependenciesOf(extension));
        map.init();

        assertThat(map.lookup(extension.getProviderDescriptor())).isEqualTo(extension);
        assertThat(map.lookup(extension.getProviderDescriptor().name())).isEqualTo(extension);
        assertThat(map.lookup(IndexType.RANGE)).containsExactlyInAnyOrder(extension, rangeIndexProvider);
        var accepted = new ArrayList<>();
        map.accept(accepted::add);
        assertThat(accepted).contains(extension);
    }

    @Test
    void testLookupByMissingType() throws Exception {
        var rangeIndexProvider = mockProvider(RangeIndexProvider.class, IndexType.RANGE);
        var map = new StaticIndexProviderMap(
                mockProvider(TokenIndexProvider.class, IndexType.LOOKUP),
                rangeIndexProvider,
                mockProvider(PointIndexProvider.class, IndexType.TEXT), // <- Specifically NOT point
                mockProvider(TextIndexProvider.class, IndexType.TEXT),
                mockProvider(TrigramIndexProvider.class, IndexType.TEXT),
                mockProvider(FulltextIndexProvider.class, IndexType.FULLTEXT),
                mockProvider(VectorIndexProvider.class, IndexType.VECTOR),
                new Dependencies());
        map.init();

        assertThatThrownBy(() -> map.lookup(IndexType.POINT))
                .isInstanceOf(IndexProviderNotFoundException.class)
                .hasMessageContaining("Tried to get index providers for index type " + IndexType.POINT
                        + " but could not find any. Available index providers per type are ")
                .hasMessageContaining(IndexType.RANGE + "=["
                        + rangeIndexProvider.getProviderDescriptor().name() + "]");
    }

    private static <T extends IndexProvider> T mockProvider(Class<? extends T> clazz) {
        var mock = mock(clazz);
        when(mock.getProviderDescriptor()).thenReturn(new IndexProviderDescriptor(clazz.getName(), "o_O"));
        return mock;
    }

    private static <T extends IndexProvider> T mockProvider(Class<? extends T> clazz, IndexType indexType) {
        var mock = mockProvider(clazz);
        when(mock.getIndexType()).thenReturn(indexType);
        return mock;
    }
}
