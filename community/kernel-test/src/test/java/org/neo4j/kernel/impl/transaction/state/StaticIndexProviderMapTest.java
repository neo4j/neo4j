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
import static org.neo4j.test.LatestVersions.LATEST_KERNEL_VERSION;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.neo4j.collection.Dependencies;
import org.neo4j.internal.schema.IndexProviderDescriptor;
import org.neo4j.internal.schema.IndexType;
import org.neo4j.kernel.KernelVersion;
import org.neo4j.kernel.api.index.IndexProvider;
import org.neo4j.kernel.impl.api.index.IndexProviderNotFoundException;

class StaticIndexProviderMapTest {

    @Test
    void testGetters() throws Exception {
        IndexProvider tokenIndexProvider = mockProvider(IndexType.LOOKUP);
        IndexProvider rangeIndexProvider = mockProvider(IndexType.RANGE);
        IndexProvider pointIndexProvider = mockProvider(IndexType.POINT);
        IndexProvider textOLDIndexProvider = mockProvider(IndexType.TEXT);
        IndexProvider textNEWIndexProvider = mockProvider(IndexType.TEXT, KernelVersion.V5_0);
        IndexProvider fulltextIndexProvider = mockProvider(IndexType.FULLTEXT);
        IndexProvider vectorOLDIndexProvider = mockProvider(IndexType.VECTOR);
        IndexProvider vectorNEWIndexProvider = mockProvider(IndexType.VECTOR, KernelVersion.V5_0);
        StaticIndexProviderMap map = new StaticIndexProviderMap(
                new Dependencies(),
                asList(
                        tokenIndexProvider, rangeIndexProvider,
                        pointIndexProvider, textOLDIndexProvider,
                        textNEWIndexProvider, fulltextIndexProvider,
                        vectorOLDIndexProvider, vectorNEWIndexProvider));
        map.init();

        assertThat(map.getTokenIndexProvider(LATEST_KERNEL_VERSION)).isEqualTo(tokenIndexProvider);
        assertThat(map.getDefaultProvider(LATEST_KERNEL_VERSION)).isEqualTo(rangeIndexProvider);
        assertThat(map.getTextIndexProvider(LATEST_KERNEL_VERSION)).isEqualTo(textNEWIndexProvider);
        assertThat(map.getFulltextProvider(LATEST_KERNEL_VERSION)).isEqualTo(fulltextIndexProvider);
        assertThat(map.getPointIndexProvider(LATEST_KERNEL_VERSION)).isEqualTo(pointIndexProvider);
        assertThat(map.getVectorIndexProvider(LATEST_KERNEL_VERSION)).isEqualTo(vectorNEWIndexProvider);
    }

    @Test
    void testLookup() throws Exception {
        Collection<IndexProvider> indexProviders = Arrays.stream(IndexType.values())
                .map(StaticIndexProviderMapTest::mockProvider)
                .toList();
        StaticIndexProviderMap map = new StaticIndexProviderMap(new Dependencies(), indexProviders);
        map.init();

        var indexProviderDescriptorAssert = assertThat(indexProviders).map(IndexProvider::getProviderDescriptor);

        indexProviderDescriptorAssert
                .as("lookup by descriptor")
                .map(map::lookup)
                .containsExactlyElementsOf(indexProviders);

        indexProviderDescriptorAssert
                .as("lookup by descriptor name")
                .map(IndexProviderDescriptor::name)
                .map(map::lookup)
                .containsExactlyElementsOf(indexProviders);
    }

    @Test
    void testAccept() throws Exception {
        Collection<IndexProvider> indexProviders = Arrays.stream(IndexType.values())
                .map(StaticIndexProviderMapTest::mockProvider)
                .toList();
        StaticIndexProviderMap map = new StaticIndexProviderMap(new Dependencies(), indexProviders);
        map.init();

        Collection<IndexProvider> accepted = new ArrayList<>();
        map.accept(accepted::add);

        assertThat(accepted).containsExactlyInAnyOrderElementsOf(indexProviders);
    }

    @Test
    void testWithExtension() throws Exception {
        IndexProvider extension = mockProvider(IndexType.RANGE);
        IndexProvider rangeIndexProvider = mockProvider(IndexType.RANGE);
        StaticIndexProviderMap map = new StaticIndexProviderMap(
                Dependencies.dependenciesOf(extension),
                asList(
                        mockProvider(IndexType.LOOKUP),
                        rangeIndexProvider,
                        mockProvider(IndexType.POINT),
                        mockProvider(IndexType.TEXT),
                        mockProvider(IndexType.FULLTEXT),
                        mockProvider(IndexType.VECTOR)));
        map.init();

        assertThat(map.lookup(extension.getProviderDescriptor())).isEqualTo(extension);
        assertThat(map.lookup(extension.getProviderDescriptor().name())).isEqualTo(extension);
        assertThat(map.lookup(IndexType.RANGE)).containsExactlyInAnyOrder(extension, rangeIndexProvider);
        Collection<IndexProvider> accepted = new ArrayList<>();
        map.accept(accepted::add);
        assertThat(accepted).contains(extension);
    }

    @Test
    void testLookupByMissingType() throws Exception {
        IndexProvider rangeIndexProvider = mockProvider(IndexType.RANGE);
        StaticIndexProviderMap map = new StaticIndexProviderMap(
                new Dependencies(),
                asList(
                        mockProvider(IndexType.LOOKUP),
                        rangeIndexProvider,
                        mockProvider(IndexType.TEXT),
                        mockProvider(IndexType.FULLTEXT),
                        mockProvider(IndexType.VECTOR)));
        map.init();

        assertThatThrownBy(() -> map.lookup(IndexType.POINT))
                .isInstanceOf(IndexProviderNotFoundException.class)
                .hasMessageContainingAll(
                        "Tried to get index providers for index type ",
                        IndexType.POINT.name(),
                        "but could not find any",
                        "Available index providers per type are",
                        "%s=[%s]"
                                .formatted(
                                        IndexType.RANGE,
                                        rangeIndexProvider
                                                .getProviderDescriptor()
                                                .name()));
    }

    @Test
    void testKernelVersion() throws Exception {
        IndexProvider provider40 = mockProvider(IndexType.LOOKUP, KernelVersion.V4_0);
        IndexProvider provider42 = mockProvider(IndexType.LOOKUP, KernelVersion.V4_2);
        IndexProvider providerGF = mockProvider(IndexType.LOOKUP, KernelVersion.GLORIOUS_FUTURE);
        IndexProvider provider50 = mockProvider(IndexType.LOOKUP, KernelVersion.V5_0);
        IndexProvider provider202505 = mockProvider(IndexType.LOOKUP, KernelVersion.V2025_05);
        IndexProvider provider525 = mockProvider(IndexType.LOOKUP, KernelVersion.V5_25);

        StaticIndexProviderMap map = new StaticIndexProviderMap(
                new Dependencies(),
                asList(provider40, provider42, providerGF, provider50, provider202505, provider525));
        map.init();

        assertThat(map.getTokenIndexProvider(KernelVersion.V4_0)).isEqualTo(provider40);
        assertThat(map.getTokenIndexProvider(KernelVersion.V4_4)).isEqualTo(provider42);
        assertThat(map.getTokenIndexProvider(KernelVersion.V5_0)).isEqualTo(provider50);
        assertThat(map.getTokenIndexProvider(KernelVersion.V5_25)).isEqualTo(provider525);
        assertThat(map.getTokenIndexProvider(KernelVersion.V2025_04)).isEqualTo(provider525);
        assertThat(map.getTokenIndexProvider(KernelVersion.V2025_05)).isEqualTo(provider202505);
        assertThat(map.getTokenIndexProvider(KernelVersion.GLORIOUS_FUTURE)).isEqualTo(providerGF);
    }

    private static IndexProvider mockProvider(IndexType indexType) {
        return mockProvider(indexType, KernelVersion.EARLIEST);
    }

    private static IndexProvider mockProvider(IndexType indexType, KernelVersion kernelVersion) {
        IndexProvider mock = mock(IndexProvider.class);
        String version = UUID.randomUUID().toString();
        when(mock.getProviderDescriptor())
                .thenReturn(new IndexProviderDescriptor(IndexProvider.class.getName(), version));
        when(mock.getIndexType()).thenReturn(indexType);
        when(mock.getMinimumRequiredVersion()).thenReturn(kernelVersion);
        return mock;
    }
}
