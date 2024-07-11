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

import static org.neo4j.kernel.api.impl.schema.TextDocumentStructure.NODE_ID_KEY;

import java.io.IOException;
import java.io.UncheckedIOException;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TotalHitCountCollector;
import org.neo4j.internal.kernel.api.IndexQueryConstraints;
import org.neo4j.internal.kernel.api.PropertyIndexQuery;
import org.neo4j.internal.kernel.api.PropertyIndexQuery.ExactPredicate;
import org.neo4j.internal.kernel.api.PropertyIndexQuery.StringContainsPredicate;
import org.neo4j.internal.kernel.api.PropertyIndexQuery.StringPrefixPredicate;
import org.neo4j.internal.kernel.api.PropertyIndexQuery.StringSuffixPredicate;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.io.pagecache.context.CursorContext;
import org.neo4j.kernel.api.impl.index.SearcherReference;
import org.neo4j.kernel.api.impl.schema.AbstractTextIndexReader;
import org.neo4j.kernel.api.impl.schema.TaskCoordinator;
import org.neo4j.kernel.api.impl.schema.TextDocumentStructure;
import org.neo4j.kernel.api.impl.schema.sampler.LuceneIndexSampler;
import org.neo4j.kernel.api.index.IndexSampler;
import org.neo4j.kernel.impl.api.index.IndexSamplingConfig;
import org.neo4j.kernel.impl.index.schema.IndexUsageTracking;
import org.neo4j.values.storable.Value;

/**
 * Schema index reader that is able to read/sample a single partition of a partitioned Lucene index.
 *
 * @see PartitionedValueIndexReader
 */
public class TextIndexReader extends AbstractTextIndexReader {
    private final IndexSamplingConfig samplingConfig;
    private final TaskCoordinator taskCoordinator;

    public TextIndexReader(
            SearcherReference searcherReference,
            IndexDescriptor descriptor,
            IndexSamplingConfig samplingConfig,
            TaskCoordinator taskCoordinator,
            IndexUsageTracking usageTracker) {
        super(descriptor, searcherReference, usageTracker);
        this.samplingConfig = samplingConfig;
        this.taskCoordinator = taskCoordinator;
    }

    @Override
    public IndexSampler createSampler() {
        return new LuceneIndexSampler(getIndexSearcher(), taskCoordinator, samplingConfig);
    }

    @Override
    protected Query toLuceneQuery(PropertyIndexQuery predicate, IndexQueryConstraints constraints) {
        return switch (predicate.type()) {
            case ALL_ENTRIES -> TextDocumentStructure.newScanQuery();
            case EXACT -> TextDocumentStructure.newSeekQuery(((ExactPredicate) predicate).value());
            case STRING_PREFIX -> {
                final var spp = (StringPrefixPredicate) predicate;
                yield CypherStringQueryFactory.stringPrefix(spp.prefix().stringValue());
            }
            case STRING_CONTAINS -> {
                final var scp = (StringContainsPredicate) predicate;
                yield CypherStringQueryFactory.stringContains(scp.contains().stringValue());
            }
            case STRING_SUFFIX -> {
                final var ssp = (StringSuffixPredicate) predicate;
                yield CypherStringQueryFactory.stringSuffix(ssp.suffix().stringValue());
            }
            default -> throw invalidQuery(IllegalArgumentException::new, predicate);
        };
    }

    @Override
    protected String entityIdFieldKey() {
        return NODE_ID_KEY;
    }

    @Override
    protected boolean needStoreFilter(PropertyIndexQuery predicate) {
        return false;
    }

    @Override
    public long countIndexedEntities(
            long entityId, CursorContext cursorContext, int[] propertyKeyIds, Value... propertyValues) {
        final var entityIdQuery = new TermQuery(TextDocumentStructure.newTermForChangeOrRemove(entityId));
        final var valueQuery = TextDocumentStructure.newSeekQuery(propertyValues);
        final var entityIdAndValueQuery = new BooleanQuery.Builder();
        entityIdAndValueQuery.add(entityIdQuery, BooleanClause.Occur.MUST);
        entityIdAndValueQuery.add(valueQuery, BooleanClause.Occur.MUST);
        try {
            final var collector = new TotalHitCountCollector();
            getIndexSearcher().search(entityIdAndValueQuery.build(), collector);
            // A <label,propertyKeyId,nodeId> tuple should only match at most a single propertyValue
            return collector.getTotalHits();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
