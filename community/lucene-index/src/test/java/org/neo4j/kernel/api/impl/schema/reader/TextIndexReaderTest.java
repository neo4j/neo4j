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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.neo4j.internal.kernel.api.IndexQueryConstraints.unconstrained;
import static org.neo4j.internal.kernel.api.PropertyIndexQuery.range;
import static org.neo4j.kernel.impl.index.schema.IndexUsageTracking.NO_USAGE_TRACKING;
import static org.neo4j.values.storable.Values.stringValue;

import java.io.IOException;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.MultiTermQuery;
import org.apache.lucene.search.TotalHitCountCollector;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.configuration.Config;
import org.neo4j.graphdb.schema.IndexType;
import org.neo4j.internal.kernel.api.PropertyIndexQuery;
import org.neo4j.internal.kernel.api.QueryContext;
import org.neo4j.internal.kernel.api.exceptions.schema.IndexNotApplicableKernelException;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.internal.schema.IndexPrototype;
import org.neo4j.internal.schema.SchemaDescriptor;
import org.neo4j.internal.schema.SchemaDescriptors;
import org.neo4j.io.pagecache.context.CursorContext;
import org.neo4j.kernel.api.impl.index.collector.DocValuesCollector;
import org.neo4j.kernel.api.impl.index.partition.Neo4jIndexSearcher;
import org.neo4j.kernel.api.impl.index.partition.PartitionSearcher;
import org.neo4j.kernel.api.impl.schema.TaskCoordinator;
import org.neo4j.kernel.api.impl.schema.TextIndexProvider;
import org.neo4j.kernel.api.impl.schema.sampler.LuceneIndexSampler;
import org.neo4j.kernel.api.index.IndexReader;
import org.neo4j.kernel.api.index.ValueIndexReader;
import org.neo4j.kernel.impl.api.index.IndexSamplingConfig;
import org.neo4j.kernel.impl.index.schema.NodeValueIterator;
import org.neo4j.values.storable.Values;

class TextIndexReaderTest {
    private static final SchemaDescriptor SCHEMA = SchemaDescriptors.forLabel(0, 0);
    private final PartitionSearcher partitionSearcher = mock(PartitionSearcher.class);
    private final Neo4jIndexSearcher indexSearcher = mock(Neo4jIndexSearcher.class);
    private final IndexSamplingConfig samplingConfig = new IndexSamplingConfig(Config.defaults());
    private final TaskCoordinator taskCoordinator = new TaskCoordinator();

    @BeforeEach
    void setUp() {
        when(partitionSearcher.getIndexSearcher()).thenReturn(indexSearcher);
    }

    @Test
    void releaseSearcherOnClose() throws IOException {
        IndexReader simpleIndexReader = getNonUniqueSimpleReader();

        simpleIndexReader.close();

        verify(partitionSearcher).close();
    }

    @Test
    void seekQueryReachSearcher() throws Exception {
        var simpleIndexReader = getNonUniqueSimpleReader();

        doQuery(simpleIndexReader, PropertyIndexQuery.exact(1, "test"));

        verify(indexSearcher).search(any(BooleanQuery.class), any(DocValuesCollector.class));
    }

    @Test
    void scanQueryReachSearcher() throws Exception {
        var simpleIndexReader = getNonUniqueSimpleReader();

        doQuery(simpleIndexReader, PropertyIndexQuery.allEntries());

        verify(indexSearcher).search(any(MatchAllDocsQuery.class), any(DocValuesCollector.class));
    }

    @Test
    void prefixRangeSeekQueryReachSearcher() throws Exception {
        var simpleIndexReader = getNonUniqueSimpleReader();

        doQuery(simpleIndexReader, PropertyIndexQuery.stringPrefix(1, stringValue("bb")));

        verify(indexSearcher).search(any(MultiTermQuery.class), any(DocValuesCollector.class));
    }

    @Test
    void numberRangeSeekQueryReachSearcher() {
        var simpleIndexReader = getNonUniqueSimpleReader();
        var query = range(1, 7, true, 8, true);

        assertThatThrownBy(() -> doQuery(simpleIndexReader, query))
                .isInstanceOf(IndexNotApplicableKernelException.class)
                .hasMessageContainingAll(
                        "Index query not supported for", IndexType.RANGE.name(), "index", "Query", query.toString());
    }

    @Test
    void countIndexedNodesReachSearcher() throws IOException {
        var simpleIndexReader = getNonUniqueSimpleReader();

        simpleIndexReader.countIndexedEntities(2, CursorContext.NULL_CONTEXT, new int[] {3}, Values.of("testValue"));

        verify(indexSearcher).search(any(BooleanQuery.class), any(TotalHitCountCollector.class));
    }

    @Test
    void nonUniqueIndexSamplerForNonUniqueIndex() {
        TextIndexReader uniqueSimpleReader = getNonUniqueSimpleReader();
        assertThat(uniqueSimpleReader.createSampler()).isInstanceOf(LuceneIndexSampler.class);
    }

    private static void doQuery(ValueIndexReader reader, PropertyIndexQuery query)
            throws IndexNotApplicableKernelException {
        reader.query(new NodeValueIterator(), QueryContext.NULL_CONTEXT, unconstrained(), query);
    }

    private TextIndexReader getNonUniqueSimpleReader() {
        IndexDescriptor index = IndexPrototype.forSchema(SCHEMA)
                .withName("a")
                .materialise(0)
                .withIndexCapability(TextIndexProvider.CAPABILITY);
        return new TextIndexReader(partitionSearcher, index, samplingConfig, taskCoordinator, NO_USAGE_TRACKING);
    }
}
