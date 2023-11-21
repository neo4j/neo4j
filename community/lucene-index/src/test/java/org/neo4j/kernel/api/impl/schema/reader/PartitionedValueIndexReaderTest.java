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
package org.neo4j.kernel.api.impl.schema.reader;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.neo4j.internal.kernel.api.IndexQueryConstraints.unconstrained;
import static org.neo4j.internal.schema.SchemaDescriptors.forLabel;
import static org.neo4j.kernel.impl.index.schema.IndexUsageTracker.NO_USAGE_TRACKER;
import static org.neo4j.values.storable.Values.stringValue;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Stream;
import org.eclipse.collections.api.list.primitive.BooleanList;
import org.eclipse.collections.api.set.primitive.LongSet;
import org.eclipse.collections.impl.factory.primitive.BooleanLists;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.neo4j.collection.PrimitiveLongCollections;
import org.neo4j.internal.kernel.api.PropertyIndexQuery;
import org.neo4j.internal.kernel.api.QueryContext;
import org.neo4j.internal.kernel.api.exceptions.schema.IndexNotApplicableKernelException;
import org.neo4j.internal.kernel.api.exceptions.schema.IndexNotFoundKernelException;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.internal.schema.IndexPrototype;
import org.neo4j.io.pagecache.context.CursorContext;
import org.neo4j.kernel.api.index.BridgingIndexProgressor;
import org.neo4j.kernel.api.index.IndexSample;
import org.neo4j.kernel.api.index.IndexSampler;
import org.neo4j.kernel.api.index.ValueIndexReader;
import org.neo4j.kernel.impl.index.schema.GatheringNodeValueClient;
import org.neo4j.kernel.impl.index.schema.NodeIdsIndexReaderQueryAnswer;
import org.neo4j.kernel.impl.index.schema.NodeValueIterator;
import org.neo4j.values.storable.Values;

class PartitionedValueIndexReaderTest {
    private static final int PROP_KEY = 1;
    private static final int LABEL_ID = 0;

    private final IndexDescriptor schemaIndexDescriptor = IndexPrototype.forSchema(forLabel(LABEL_ID, PROP_KEY))
            .withName("index")
            .materialise(0);
    private final TextIndexReader indexReader1 = mock(TextIndexReader.class);
    private final TextIndexReader indexReader2 = mock(TextIndexReader.class);
    private final TextIndexReader indexReader3 = mock(TextIndexReader.class);

    @Test
    void partitionedReaderCloseAllReaders() {
        PartitionedValueIndexReader partitionedIndexReader = createPartitionedReader();

        partitionedIndexReader.close();

        verify(indexReader1).close();
        verify(indexReader2).close();
        verify(indexReader3).close();
    }

    @Test
    void seekOverAllPartitions() throws Exception {
        PartitionedValueIndexReader indexReader = createPartitionedReaderFromReaders();

        PropertyIndexQuery.ExactPredicate query = PropertyIndexQuery.exact(1, "Test");
        doAnswer(new NodeIdsIndexReaderQueryAnswer(schemaIndexDescriptor, 1))
                .when(indexReader1)
                .query(any(), any(), any(), any());
        doAnswer(new NodeIdsIndexReaderQueryAnswer(schemaIndexDescriptor, 2))
                .when(indexReader2)
                .query(any(), any(), any(), any());
        doAnswer(new NodeIdsIndexReaderQueryAnswer(schemaIndexDescriptor, 3))
                .when(indexReader3)
                .query(any(), any(), any(), any());

        LongSet results = queryResultAsSet(indexReader, query);
        verifyResult(results);
    }

    @Test
    void rangeSeekByNumberOverPartitions() throws Exception {
        PartitionedValueIndexReader indexReader = createPartitionedReaderFromReaders();

        PropertyIndexQuery.RangePredicate<?> query = PropertyIndexQuery.range(1, 1, true, 2, true);
        doAnswer(new NodeIdsIndexReaderQueryAnswer(schemaIndexDescriptor, 1))
                .when(indexReader1)
                .query(any(), any(), any(), any());
        doAnswer(new NodeIdsIndexReaderQueryAnswer(schemaIndexDescriptor, 2))
                .when(indexReader2)
                .query(any(), any(), any(), any());
        doAnswer(new NodeIdsIndexReaderQueryAnswer(schemaIndexDescriptor, 3))
                .when(indexReader3)
                .query(any(), any(), any(), any());

        LongSet results = queryResultAsSet(indexReader, query);
        verifyResult(results);
    }

    @Test
    void rangeSeekByStringOverPartitions() throws Exception {
        PartitionedValueIndexReader indexReader = createPartitionedReaderFromReaders();

        PropertyIndexQuery.RangePredicate<?> query = PropertyIndexQuery.range(1, "a", false, "b", true);
        doAnswer(new NodeIdsIndexReaderQueryAnswer(schemaIndexDescriptor, 1))
                .when(indexReader1)
                .query(any(), any(), any(), any());
        doAnswer(new NodeIdsIndexReaderQueryAnswer(schemaIndexDescriptor, 2))
                .when(indexReader2)
                .query(any(), any(), any(), any());
        doAnswer(new NodeIdsIndexReaderQueryAnswer(schemaIndexDescriptor, 3))
                .when(indexReader3)
                .query(any(), any(), any(), any());

        LongSet results = queryResultAsSet(indexReader, query);
        verifyResult(results);
    }

    @Test
    void rangeSeekByPrefixOverPartitions() throws Exception {
        PartitionedValueIndexReader indexReader = createPartitionedReaderFromReaders();
        PropertyIndexQuery.StringPrefixPredicate query = PropertyIndexQuery.stringPrefix(1, stringValue("prefix"));
        doAnswer(new NodeIdsIndexReaderQueryAnswer(schemaIndexDescriptor, 1))
                .when(indexReader1)
                .query(any(), any(), any(), any());
        doAnswer(new NodeIdsIndexReaderQueryAnswer(schemaIndexDescriptor, 2))
                .when(indexReader2)
                .query(any(), any(), any(), any());
        doAnswer(new NodeIdsIndexReaderQueryAnswer(schemaIndexDescriptor, 3))
                .when(indexReader3)
                .query(any(), any(), any(), any());

        LongSet results = queryResultAsSet(indexReader, query);
        verifyResult(results);
    }

    @Test
    void scanOverPartitions() throws Exception {
        PartitionedValueIndexReader indexReader = createPartitionedReaderFromReaders();
        PropertyIndexQuery.ExistsPredicate query = PropertyIndexQuery.exists(1);
        doAnswer(new NodeIdsIndexReaderQueryAnswer(schemaIndexDescriptor, 1))
                .when(indexReader1)
                .query(any(), any(), any(), any());
        doAnswer(new NodeIdsIndexReaderQueryAnswer(schemaIndexDescriptor, 2))
                .when(indexReader2)
                .query(any(), any(), any(), any());
        doAnswer(new NodeIdsIndexReaderQueryAnswer(schemaIndexDescriptor, 3))
                .when(indexReader3)
                .query(any(), any(), any(), any());

        LongSet results = queryResultAsSet(indexReader, query);
        verifyResult(results);
    }

    @Test
    void countNodesOverPartitions() {
        PartitionedValueIndexReader indexReader = createPartitionedReaderFromReaders();
        when(indexReader1.countIndexedEntities(1, CursorContext.NULL_CONTEXT, new int[] {PROP_KEY}, Values.of("a")))
                .thenReturn(1L);
        when(indexReader2.countIndexedEntities(1, CursorContext.NULL_CONTEXT, new int[] {PROP_KEY}, Values.of("a")))
                .thenReturn(2L);
        when(indexReader3.countIndexedEntities(1, CursorContext.NULL_CONTEXT, new int[] {PROP_KEY}, Values.of("a")))
                .thenReturn(3L);

        assertEquals(
                6,
                indexReader.countIndexedEntities(1, CursorContext.NULL_CONTEXT, new int[] {PROP_KEY}, Values.of("a")));
    }

    @Test
    void samplingOverPartitions() throws IndexNotFoundKernelException {
        PartitionedValueIndexReader indexReader = createPartitionedReaderFromReaders();
        when(indexReader1.createSampler()).thenReturn(new SimpleSampler(1));
        when(indexReader2.createSampler()).thenReturn(new SimpleSampler(2));
        when(indexReader3.createSampler()).thenReturn(new SimpleSampler(3));

        IndexSampler sampler = indexReader.createSampler();
        assertEquals(new IndexSample(6, 6, 6), sampler.sampleIndex(CursorContext.NULL_CONTEXT, new AtomicBoolean()));
    }

    @ParameterizedTest
    @MethodSource("needStoreFilters")
    void propagateNeedStoreFilter1(BooleanList needStoreFilters, boolean needStoreFilter)
            throws IndexNotApplicableKernelException {
        var query = mock(PropertyIndexQuery.class);
        var client = new GatheringNodeValueClient();

        // Update mocked sub-readers with value for needStoreFilter
        setNeedStoreFilter(indexReader1, needStoreFilters.get(0));
        setNeedStoreFilter(indexReader2, needStoreFilters.get(1));
        setNeedStoreFilter(indexReader3, needStoreFilters.get(2));

        PartitionedValueIndexReader indexReader = createPartitionedReaderFromReaders();
        indexReader.query(client, QueryContext.NULL_CONTEXT, unconstrained(), query);

        assertThat(client.needStoreFilter).isEqualTo(needStoreFilter);
    }

    private void setNeedStoreFilter(TextIndexReader indexReader, boolean needStoreFilter)
            throws IndexNotApplicableKernelException {
        doAnswer(invocation -> {
                    // This is out outer client
                    var invokedClient = (BridgingIndexProgressor) invocation.getArgument(0);
                    invokedClient.initialize(
                            schemaIndexDescriptor, invokedClient, false, needStoreFilter, null, (PropertyIndexQuery)
                                    null);
                    return null;
                })
                .when(indexReader)
                .query(any(), any(), any(), any());
    }

    public static Stream<Arguments> needStoreFilters() {
        return Stream.of(
                        BooleanLists.immutable.of(false, false, false),
                        BooleanLists.immutable.of(true, false, false),
                        BooleanLists.immutable.of(false, true, false),
                        BooleanLists.immutable.of(false, false, true),
                        BooleanLists.immutable.of(true, true, true))
                .map(bs -> Arguments.of(bs, bs.contains(true)));
    }

    private static LongSet queryResultAsSet(PartitionedValueIndexReader indexReader, PropertyIndexQuery query)
            throws IndexNotApplicableKernelException {
        try (NodeValueIterator iterator = new NodeValueIterator()) {
            indexReader.query(iterator, QueryContext.NULL_CONTEXT, unconstrained(), query);
            return PrimitiveLongCollections.asSet(iterator);
        }
    }

    private static void verifyResult(LongSet results) {
        assertEquals(3, results.size());
        assertTrue(results.contains(1));
        assertTrue(results.contains(2));
        assertTrue(results.contains(3));
    }

    private PartitionedValueIndexReader createPartitionedReaderFromReaders() {
        return new PartitionedValueIndexReader(schemaIndexDescriptor, getPartitionReaders(), NO_USAGE_TRACKER);
    }

    private List<ValueIndexReader> getPartitionReaders() {
        return Arrays.asList(indexReader1, indexReader2, indexReader3);
    }

    private PartitionedValueIndexReader createPartitionedReader() {
        return new PartitionedValueIndexReader(schemaIndexDescriptor, getPartitionReaders(), NO_USAGE_TRACKER);
    }

    private static class SimpleSampler implements IndexSampler {
        private final long sampleValue;

        SimpleSampler(long sampleValue) {
            this.sampleValue = sampleValue;
        }

        @Override
        public IndexSample sampleIndex(CursorContext cursorContext, AtomicBoolean stopped) {
            return new IndexSample(sampleValue, sampleValue, sampleValue);
        }
    }
}
