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
package org.neo4j.kernel.api.impl.index.lucene.v10;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.InstanceOfAssertFactories.FLOAT_ARRAY;
import static org.assertj.core.api.InstanceOfAssertFactories.INTEGER;
import static org.assertj.core.api.InstanceOfAssertFactories.type;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.neo4j.kernel.api.impl.schema.vector.Neo4jVectorSimilarityFunction.EUCLIDEAN;

import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.stream.Stream;
import org.apache.lucene.analysis.core.KeywordAnalyzer;
import org.apache.lucene.search.BooleanClause.Occur;
import org.apache.lucene.search.BooleanQuery.Builder;
import org.apache.lucene.search.KnnFloatVectorQuery;
import org.apache.lucene.search.MatchNoDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.RescoreTopNQuery;
import org.assertj.core.api.ObjectAssert;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.neo4j.configuration.Config;
import org.neo4j.internal.kernel.api.IndexQueryConstraints;
import org.neo4j.internal.kernel.api.PropertyIndexQuery;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.kernel.api.impl.index.collector.ValuesIterator;
import org.neo4j.kernel.api.impl.index.lucene.LuceneContext;
import org.neo4j.kernel.api.impl.index.lucene.LuceneDirectory;
import org.neo4j.kernel.api.impl.index.lucene.LuceneDirectoryReader;
import org.neo4j.kernel.api.impl.index.lucene.LuceneDocument;
import org.neo4j.kernel.api.impl.index.lucene.LuceneIndexSearcher;
import org.neo4j.kernel.api.impl.index.lucene.LuceneIndexWriter;
import org.neo4j.kernel.api.impl.index.lucene.LuceneIndexWriterConfig;
import org.neo4j.kernel.api.impl.index.lucene.LuceneQueryContext;
import org.neo4j.kernel.api.impl.index.lucene.LuceneSettings;
import org.neo4j.kernel.api.impl.index.storage.DirectoryFactory;
import org.neo4j.kernel.api.impl.schema.LuceneQueryFactory.VectorQueryFactory;
import org.neo4j.kernel.api.impl.schema.vector.VectorDocumentStructure;
import org.neo4j.kernel.api.impl.schema.vector.VectorQuantizationType;
import org.neo4j.test.RandomSupport;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.RandomSupportExtension;
import org.neo4j.values.storable.Values;

@RandomSupportExtension
public class Lucene10RescoringQueryTest {
    private static final int DIMENSIONS = 1024;
    private static final String EMBEDDING_FIELD = "embedding";

    private static final int TOP_K = 10;
    private static final double DEFAULT_SEARCH_EXPANSION = 8.0;
    private static final int MAX_EF_SEARCH = Config.defaults().get(LuceneSettings.vector_hnsw_max_ef_search);

    private static final LuceneIndexWriterConfig WRITER_CONFIG =
            LuceneIndexWriterConfig.analyzerOnly(new KeywordAnalyzer());
    private static final VectorDocumentStructure DOCUMENT_STRUCTURE = mock(VectorDocumentStructure.class);
    private static final IndexQueryConstraints QUERY_CONSTRAINTS =
            IndexQueryConstraints.unconstrained().limit(TOP_K);

    static {
        when(DOCUMENT_STRUCTURE.vectorValueKeyFor(DIMENSIONS)).thenReturn(EMBEDDING_FIELD);
    }

    @Inject
    private RandomSupport random;

    private static final Collection<Params> ANN_QUERY_PARAMETERS = List.of(
            new Params(VectorQuantizationType.NONE, 0.5),
            new Params(VectorQuantizationType.NONE, 1.0),
            new Params(VectorQuantizationType.NONE, Math.TAU),
            new Params(VectorQuantizationType.NONE, 10.0),
            new Params(VectorQuantizationType.NONE, Double.NaN),
            new Params(VectorQuantizationType.SCALAR, 0.5),
            new Params(VectorQuantizationType.SCALAR, 1.0),
            new Params(VectorQuantizationType.SCALAR, Math.TAU),
            new Params(VectorQuantizationType.SCALAR, 10.0),
            new Params(VectorQuantizationType.SCALAR, Double.NaN),
            new Params(VectorQuantizationType.BINARY, 0.5),
            new Params(VectorQuantizationType.BINARY, 1.0),
            new Params(VectorQuantizationType.BINARY, Math.TAU),
            new Params(VectorQuantizationType.BINARY, 10.0),
            new Params(VectorQuantizationType.BINARY, Double.NaN));

    private static Stream<Params> annQueryParameters() {
        return ANN_QUERY_PARAMETERS.stream();
    }

    private static final class Params {
        final VectorQuantizationType quantizationType;
        final double searchExpansionFactor;
        final int efSearch;
        final boolean shouldRescore;

        Params(VectorQuantizationType quantizationType, double searchExpansionFactor) {
            double effectiveSearchExpansionFactor =
                    Double.isNaN(searchExpansionFactor) ? DEFAULT_SEARCH_EXPANSION : searchExpansionFactor;
            this.quantizationType = quantizationType;
            this.searchExpansionFactor = searchExpansionFactor;
            this.efSearch = Math.max((int) Math.ceil(effectiveSearchExpansionFactor * TOP_K), TOP_K);
            this.shouldRescore = !Objects.equals(quantizationType, VectorQuantizationType.NONE) && efSearch > TOP_K;
        }

        @Override
        public String toString() {
            return getClass().getSimpleName() + '['
                    + "quantizationType=" + quantizationType
                    + ", searchExpansionFactor=" + searchExpansionFactor
                    + ", shouldRescore=" + shouldRescore + ", efSearch="
                    + efSearch
                    + ']';
        }
    }

    @ParameterizedTest
    @MethodSource("annQueryParameters")
    void testAnnQuery(Params params) throws Exception {
        float[] embedding = randomEmbedding();

        try (DirectoryFactory directoryFactory = newInMemoryDirectoryFactory();
                LuceneDirectory directory = directoryFactory.open(null)) {

            // creates an empty index so that opening the indexReader doesn't crash
            try (LuceneIndexWriter indexWriter = directory.newWriter(WRITER_CONFIG)) {
                indexWriter.commit();
            }

            try (LuceneDirectoryReader indexReader = directory.open();
                    LuceneIndexSearcher indexSearcher = indexReader.newDirectSearcher()) {

                LuceneQueryContext queryContext = vectorQueryFactory(params.quantizationType)
                        .createQuery(
                                indexSearcher,
                                QUERY_CONSTRAINTS,
                                IndexDescriptor.NO_INDEX,
                                PropertyIndexQuery.nearestNeighbors(TOP_K, params.searchExpansionFactor, embedding));

                Query query = query(queryContext);

                if (params.shouldRescore) {
                    ObjectAssert<RescoreTopNQuery> rescoreQueryAssert =
                            assertThat(query).extracting("delegate", type(RescoreTopNQuery.class));
                    rescoreQueryAssert.extracting("n", INTEGER).isEqualTo(TOP_K);
                    assertKnnQuery(
                            rescoreQueryAssert
                                    .extracting("query", type(Query.class))
                                    .extracting("delegate", type(KnnFloatVectorQuery.class)),
                            params.efSearch,
                            embedding);
                } else {
                    assertKnnQuery(assertThat(query), params.efSearch, embedding);
                }

                ValuesIterator valuesIterator = indexSearcher.searchVectors(queryContext, QUERY_CONSTRAINTS);
                assertThat(valuesIterator.hasNext()).as("empty index").isFalse();
            }
        }
    }

    private static VectorQueryFactory vectorQueryFactory(VectorQuantizationType quantizationType) {
        return new VectorQueryFactory(DOCUMENT_STRUCTURE, quantizationType, DEFAULT_SEARCH_EXPANSION, MAX_EF_SEARCH);
    }

    @ParameterizedTest
    @MethodSource("annQueryParameters")
    void testRescoringOnEmptyResults(Params params) throws Exception {
        float[] embedding = randomEmbedding();

        try (DirectoryFactory directoryFactory = newInMemoryDirectoryFactory();
                LuceneDirectory directory = directoryFactory.open(null)) {

            // creates an empty index so that opening the indexReader doesn't crash
            try (LuceneIndexWriter indexWriter = directory.newWriter(WRITER_CONFIG)) {
                // segment 1
                {
                    LuceneDocument doc = indexWriter.newDocument();
                    doc.addStringField("id", "1", true);
                    doc.addKnnFloatVectorField(EMBEDDING_FIELD, embedding, EUCLIDEAN);
                    indexWriter.addDocument(doc);
                    indexWriter.commit();
                }
                // segment 2
                {
                    LuceneDocument doc = indexWriter.newDocument();
                    doc.addStringField("id", "2", true);
                    doc.addKnnFloatVectorField(EMBEDDING_FIELD, embedding, EUCLIDEAN);
                    indexWriter.addDocument(doc);
                    indexWriter.commit();
                }
            }

            try (LuceneDirectoryReader indexReader = directory.open();
                    LuceneIndexSearcher indexSearcher = indexReader.newDirectSearcher()) {

                LuceneQueryContext queryContext = vectorQueryFactory(params.quantizationType)
                        .createQuery(
                                indexSearcher,
                                QUERY_CONSTRAINTS,
                                IndexDescriptor.NO_INDEX,
                                PropertyIndexQuery.nearestNeighbors(TOP_K, params.searchExpansionFactor, embedding),
                                PropertyIndexQuery.exact(0, Values.NaN) // never matches
                                );

                Query query = query(queryContext);
                Query emptyFilter = new Builder()
                        .add(MatchNoDocsQuery.INSTANCE, Occur.FILTER)
                        .build();

                if (params.shouldRescore) {
                    ObjectAssert<RescoreTopNQuery> rescoreQueryAssert =
                            assertThat(query).extracting("delegate", type(RescoreTopNQuery.class));
                    rescoreQueryAssert.extracting("n", INTEGER).isEqualTo(TOP_K);
                    assertKnnQuery(
                            rescoreQueryAssert
                                    .extracting("query", type(Query.class))
                                    .extracting("delegate", type(KnnFloatVectorQuery.class)),
                            params.efSearch,
                            embedding,
                            emptyFilter);
                } else {
                    assertKnnQuery(assertThat(query), params.efSearch, embedding, emptyFilter);
                }

                ValuesIterator valuesIterator = indexSearcher.searchVectors(queryContext, QUERY_CONSTRAINTS);
                assertThat(valuesIterator.hasNext()).as("non matching query").isFalse();
            }
        }
    }

    private float[] randomEmbedding() {
        float[] embedding = new float[DIMENSIONS];
        for (int i = 0; i < embedding.length; i++) {
            embedding[i] = random.nextFloat();
        }
        int index = random.nextInt(DIMENSIONS);
        if (embedding[index] == 0.0f) {
            embedding[index] = random.nextBoolean() ? Math.nextDown(0.0f) : Math.nextUp(0.0f);
        }
        return embedding;
    }

    private static DirectoryFactory newInMemoryDirectoryFactory() {
        return LuceneContext.LUCENE_10.directoryFactory().newInMemoryDirectoryFactory();
    }

    private static Query query(LuceneQueryContext queryContext) {
        return ((Lucene10QueryContext) queryContext).build();
    }

    private static void assertKnnQuery(ObjectAssert<?> queryAssert, int maximumNumberOfResults, float[] embedding) {
        assertKnnQuery(queryAssert, maximumNumberOfResults, embedding, null);
    }

    private static void assertKnnQuery(
            ObjectAssert<?> queryAssert, int maximumNumberOfResults, float[] embedding, Query expectedFilter) {
        queryAssert
                .asInstanceOf(type(KnnFloatVectorQuery.class))
                .as("number of results")
                .returns(maximumNumberOfResults, KnnFloatVectorQuery::getK)
                .as("filter")
                .returns(expectedFilter, KnnFloatVectorQuery::getFilter)
                .as("embedding field")
                .returns(EMBEDDING_FIELD, KnnFloatVectorQuery::getField)
                .extracting(KnnFloatVectorQuery::getTargetCopy, FLOAT_ARRAY)
                .as("embedding")
                .containsExactly(embedding);
    }
}
