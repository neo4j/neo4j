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
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.stream.Stream;
import org.apache.lucene.analysis.core.KeywordAnalyzer;
import org.apache.lucene.search.BooleanClause.Occur;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.KnnFloatVectorQuery;
import org.apache.lucene.search.MatchNoDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.RescoreTopNQuery;
import org.assertj.core.api.InstanceOfAssertFactories;
import org.assertj.core.api.ObjectAssert;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.neo4j.internal.kernel.api.IndexQueryConstraints;
import org.neo4j.internal.kernel.api.PropertyIndexQuery;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.kernel.api.impl.index.collector.ValuesIterator;
import org.neo4j.kernel.api.impl.index.lucene.LuceneContext;
import org.neo4j.kernel.api.impl.index.lucene.LuceneDocument;
import org.neo4j.kernel.api.impl.index.lucene.LuceneIndexWriterConfig;
import org.neo4j.kernel.api.impl.schema.LuceneQueryFactory.VectorQueryFactory;
import org.neo4j.kernel.api.impl.schema.vector.Neo4jVectorSimilarityFunction;
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

    @Inject
    private RandomSupport random;

    static Stream<Arguments> annQueryParameters() {
        return Stream.of(
                Arguments.arguments(VectorQuantizationType.NONE, 0.5, 10, false),
                Arguments.arguments(VectorQuantizationType.NONE, 1.0, 10, false),
                Arguments.arguments(VectorQuantizationType.NONE, Math.TAU, 10, false),
                Arguments.arguments(VectorQuantizationType.NONE, 10.0, 10, false),
                Arguments.arguments(VectorQuantizationType.NONE, Double.NaN, 10, false),
                Arguments.arguments(VectorQuantizationType.SCALAR, 0.5, 100, false),
                Arguments.arguments(VectorQuantizationType.SCALAR, 1.0, 10, false),
                Arguments.arguments(VectorQuantizationType.SCALAR, Math.TAU, 63, true),
                Arguments.arguments(VectorQuantizationType.SCALAR, 10.0, 100, true),
                Arguments.arguments(VectorQuantizationType.SCALAR, Double.NaN, 80, true),
                Arguments.arguments(VectorQuantizationType.BINARY, 0.5, 100, false),
                Arguments.arguments(VectorQuantizationType.BINARY, 1.0, 10, false),
                Arguments.arguments(VectorQuantizationType.BINARY, Math.TAU, 63, true),
                Arguments.arguments(VectorQuantizationType.BINARY, 10.0, 100, true),
                Arguments.arguments(VectorQuantizationType.BINARY, Double.NaN, 80, true));
    }

    @ParameterizedTest
    @MethodSource("annQueryParameters")
    void testAnnQuery(
            VectorQuantizationType quantizationType,
            double searchExpansion,
            int expectedEfSearch,
            boolean expectedRescoring)
            throws Exception {

        float[] embedding = new float[DIMENSIONS];
        for (int i = 0; i < embedding.length; i++) {
            embedding[i] = random.nextFloat();
        }

        VectorDocumentStructure documentStructure = mock(VectorDocumentStructure.class);
        when(documentStructure.vectorValueKeyFor(DIMENSIONS)).thenReturn(EMBEDDING_FIELD);

        try (var directoryFactory = LuceneContext.LUCENE_10.directoryFactory().newInMemoryDirectoryFactory();
                var directory = directoryFactory.open(null)) {

            // creates an empty index so that opening the indexReader doesn't crash
            try (var indexWriter = directory.newWriter(LuceneIndexWriterConfig.analyzerOnly(new KeywordAnalyzer()))) {
                indexWriter.commit();
            }

            try (var indexReader = directory.open();
                    var indexSearcher = indexReader.newDirectSearcher()) {

                var vectorQueryFactory =
                        new VectorQueryFactory(documentStructure, quantizationType, DEFAULT_SEARCH_EXPANSION);
                var queryContext = vectorQueryFactory.createQuery(
                        indexSearcher,
                        IndexQueryConstraints.unconstrained(),
                        IndexDescriptor.NO_INDEX,
                        PropertyIndexQuery.nearestNeighbors(TOP_K, searchExpansion, embedding));

                Query query = ((Lucene10QueryContext) queryContext).build();

                if (expectedRescoring) {
                    var rescoreQueryAssert =
                            assertThat(query).asInstanceOf(InstanceOfAssertFactories.type(RescoreTopNQuery.class));
                    rescoreQueryAssert
                            .extracting("n", InstanceOfAssertFactories.INTEGER)
                            .isEqualTo(TOP_K);
                    assertKnnQuery(
                            rescoreQueryAssert
                                    .extracting("query", InstanceOfAssertFactories.type(Query.class))
                                    .extracting("delegate", InstanceOfAssertFactories.type(KnnFloatVectorQuery.class)),
                            expectedEfSearch,
                            embedding);
                } else {
                    assertKnnQuery(assertThat(query), TOP_K, embedding);
                }
            }
        }
    }

    @ParameterizedTest
    @MethodSource("annQueryParameters")
    void testRescoringOnEmptyResults(
            VectorQuantizationType quantizationType,
            double searchExpansion,
            int expectedEfSearch,
            boolean expectedRescoring)
            throws Exception {

        float[] embedding = new float[DIMENSIONS];
        for (int i = 0; i < embedding.length; i++) {
            embedding[i] = random.nextFloat();
        }

        VectorDocumentStructure documentStructure = mock(VectorDocumentStructure.class);
        when(documentStructure.vectorValueKeyFor(DIMENSIONS)).thenReturn(EMBEDDING_FIELD);

        try (var directoryFactory = LuceneContext.LUCENE_10.directoryFactory().newInMemoryDirectoryFactory();
                var directory = directoryFactory.open(null)) {

            // creates an empty index so that opening the indexReader doesn't crash
            try (var indexWriter = directory.newWriter(LuceneIndexWriterConfig.analyzerOnly(new KeywordAnalyzer()))) {
                // segment 1
                {
                    LuceneDocument doc = indexWriter.newDocument();
                    doc.addStringField("id", "1", true);
                    doc.addKnnFloatVectorField(EMBEDDING_FIELD, embedding, Neo4jVectorSimilarityFunction.EUCLIDEAN);
                    indexWriter.addDocument(doc);
                    indexWriter.commit();
                }
                // segment 2
                {
                    LuceneDocument doc = indexWriter.newDocument();
                    doc.addStringField("id", "2", true);
                    doc.addKnnFloatVectorField(EMBEDDING_FIELD, embedding, Neo4jVectorSimilarityFunction.EUCLIDEAN);
                    indexWriter.addDocument(doc);
                    indexWriter.commit();
                }
            }

            try (var indexReader = directory.open();
                    var indexSearcher = indexReader.newDirectSearcher()) {

                var vectorQueryFactory =
                        new VectorQueryFactory(documentStructure, quantizationType, DEFAULT_SEARCH_EXPANSION);
                var queryContext = vectorQueryFactory.createQuery(
                        indexSearcher,
                        IndexQueryConstraints.unconstrained(),
                        IndexDescriptor.NO_INDEX,
                        PropertyIndexQuery.nearestNeighbors(TOP_K, searchExpansion, embedding),
                        PropertyIndexQuery.exact(0, Values.NaN) // never matches
                        );

                Query query = ((Lucene10QueryContext) queryContext).build();
                Query emptyFilter = new BooleanQuery.Builder()
                        .add(MatchNoDocsQuery.INSTANCE, Occur.FILTER)
                        .build();

                if (expectedRescoring) {
                    var rescoreQueryAssert =
                            assertThat(query).asInstanceOf(InstanceOfAssertFactories.type(RescoreTopNQuery.class));
                    rescoreQueryAssert
                            .extracting("n", InstanceOfAssertFactories.INTEGER)
                            .isEqualTo(TOP_K);
                    assertKnnQuery(
                            rescoreQueryAssert
                                    .extracting("query", InstanceOfAssertFactories.type(Query.class))
                                    .extracting("delegate", InstanceOfAssertFactories.type(KnnFloatVectorQuery.class)),
                            expectedEfSearch,
                            embedding,
                            emptyFilter);
                } else {
                    assertKnnQuery(assertThat(query), TOP_K, embedding, emptyFilter);
                }

                ValuesIterator valuesIterator = indexSearcher.searchVectors(
                        queryContext, IndexQueryConstraints.unconstrained().limit(4));

                // non matching query
                assertThat(valuesIterator.hasNext()).isFalse();
            }
        }
    }

    void assertKnnQuery(ObjectAssert<?> queryAssert, int k, float[] embedding) {
        assertKnnQuery(queryAssert, k, embedding, null);
    }

    void assertKnnQuery(ObjectAssert<?> queryAssert, int k, float[] embedding, Query expectedFilter) {
        queryAssert
                .asInstanceOf(InstanceOfAssertFactories.type(KnnFloatVectorQuery.class))
                .returns(k, KnnFloatVectorQuery::getK)
                .returns(expectedFilter, KnnFloatVectorQuery::getFilter)
                .returns(EMBEDDING_FIELD, KnnFloatVectorQuery::getField)
                .extracting(KnnFloatVectorQuery::getTargetCopy, InstanceOfAssertFactories.FLOAT_ARRAY)
                .containsExactly(embedding);
    }
}
