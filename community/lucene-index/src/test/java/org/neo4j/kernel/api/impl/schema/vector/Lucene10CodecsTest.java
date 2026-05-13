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
package org.neo4j.kernel.api.impl.schema.vector;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Named.named;
import static org.neo4j.kernel.api.impl.index.lucene.LuceneIndexWriterConfig.MergePolicyOption.LOG_BYTE_SIZED;

import java.io.IOException;
import java.util.Arrays;
import java.util.Map;
import java.util.stream.Stream;
import org.apache.lucene.analysis.core.KeywordAnalyzer;
import org.apache.lucene.codecs.Codec;
import org.apache.lucene.index.SegmentInfos;
import org.junit.jupiter.api.Named;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.neo4j.graphdb.schema.IndexSetting;
import org.neo4j.internal.schema.SettingsAccessor;
import org.neo4j.internal.schema.SettingsAccessor.IndexSettingObjectMapAccessor;
import org.neo4j.kernel.api.impl.index.lucene.LuceneContext;
import org.neo4j.kernel.api.impl.index.lucene.LuceneDirectory;
import org.neo4j.kernel.api.impl.index.lucene.LuceneDirectoryFactory;
import org.neo4j.kernel.api.impl.index.lucene.LuceneDirectoryReader;
import org.neo4j.kernel.api.impl.index.lucene.LuceneDocument;
import org.neo4j.kernel.api.impl.index.lucene.LuceneDocumentsFactory;
import org.neo4j.kernel.api.impl.index.lucene.LuceneIndexWriter;
import org.neo4j.kernel.api.impl.index.lucene.LuceneIndexWriterConfig;
import org.neo4j.kernel.api.impl.index.lucene.codec.LuceneCodec;
import org.neo4j.kernel.api.impl.index.lucene.v10.LuceneDirectoryReaderAccess;
import org.neo4j.kernel.api.impl.index.lucene.v10.codec.Lucene10Codec;
import org.neo4j.values.storable.Float32Vector;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.Values;

public class Lucene10CodecsTest {

    static Stream<Named<Boolean>> indexRadsWithSameCodecValuesAsWasWritten() {
        Named<Boolean> noQuantization = named("No quantization", false);
        Named<Boolean> scalarQuantization = named("Scalar quantization", true);
        return Stream.of(noQuantization, scalarQuantization);
    }

    @ParameterizedTest
    @MethodSource
    void indexRadsWithSameCodecValuesAsWasWritten(boolean quantizationEnabled) throws IOException {
        int dimensions = 3;
        float[] vectorValues = new float[dimensions];
        Arrays.fill(vectorValues, 0.42f);
        Float32Vector float32Vector = Values.float32Vector(vectorValues);
        Value[] values = new Value[] {float32Vector};

        SettingsAccessor indexSettings = new IndexSettingObjectMapAccessor(Map.of(
                IndexSetting.vector_Quantization_Enabled(),
                quantizationEnabled,
                IndexSetting.vector_Dimensions(),
                dimensions));
        VectorIndexConfig config =
                VectorIndexVersion.V3_0.indexSettingValidator().validateToTypedConfig(indexSettings);

        LuceneContext context = LuceneContext.LUCENE_10;
        LuceneCodec codec = context.codecsFactory().codecFor(config);
        LuceneIndexWriterConfig writerConfig = new LuceneIndexWriterConfig(new KeywordAnalyzer()).setCodec(codec);
        writerConfig.setMergingParameters(1.0, 10, LOG_BYTE_SIZED, 10, 10, 10, 8.0, 10);
        VectorDocumentStructure documentStructure =
                VectorDocumentStructures.documentStructureFor(VectorIndexVersion.V3_0);
        LuceneDocumentsFactory documentFactory = context.documentsFactory();
        LuceneDirectoryFactory directoryFactory = context.directoryFactory();

        try (LuceneDirectory directory = directoryFactory.inMemoryDirectory()) {

            // write a document with vectors
            try (LuceneIndexWriter indexWriter = directory.newWriter(writerConfig)) {
                LuceneDocument vectorDocument = documentFactory.createVectorDocument(
                        documentStructure, 1, Neo4jVectorSimilarityFunction.EUCLIDEAN, values);

                indexWriter.addDocument(vectorDocument);
                indexWriter.commit();
            }

            // open new reader, which reads the codec from the segment
            try (LuceneDirectoryReader indexReader = directory.open()) {
                SegmentInfos segmentInfos = LuceneDirectoryReaderAccess.getSegmentInfos(indexReader);
                Codec writeCodec = ((Lucene10Codec) codec).codec();
                Codec readCodec = segmentInfos.info(0).info.getCodec();
                assertThat(readCodec.getName()).isEqualTo(writeCodec.getName());
                assertThat(readCodec.knnVectorsFormat().getName())
                        .isEqualTo(writeCodec.knnVectorsFormat().getName());
            }
        }
    }
}
