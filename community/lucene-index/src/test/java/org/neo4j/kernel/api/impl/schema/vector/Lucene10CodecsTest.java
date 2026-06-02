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

import static java.util.Map.entry;
import static org.assertj.core.api.Assertions.assertThat;
import static org.neo4j.kernel.api.impl.index.lucene.LuceneIndexWriterConfig.MergePolicyOption.LOG_BYTE_SIZED;

import java.io.IOException;
import java.util.Arrays;
import java.util.Map;
import org.apache.lucene.analysis.core.KeywordAnalyzer;
import org.apache.lucene.codecs.Codec;
import org.apache.lucene.index.SegmentInfos;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.neo4j.configuration.Config;
import org.neo4j.graphdb.schema.IndexSetting;
import org.neo4j.internal.schema.SettingsAccessor;
import org.neo4j.internal.schema.SettingsAccessor.IndexSettingObjectMapAccessor;
import org.neo4j.kernel.KernelVersion;
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
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.Values;

public class Lucene10CodecsTest {
    private static final LuceneContext CONTEXT = LuceneContext.LUCENE_10;
    private static final KernelVersion KERNEL_VERSION = KernelVersion.getLatestVersion(Config.defaults());
    private static final VectorIndexVersion VECTOR_INDEX_VERSION =
            VectorIndexVersion.latestSupportedVersion(KERNEL_VERSION);

    @ParameterizedTest
    @EnumSource
    void indexRadsWithSameCodecValuesAsWasWritten(VectorQuantizationType quantizationType) throws IOException {
        int dimensions = 3;
        float[] vectorValues = new float[dimensions];
        Arrays.fill(vectorValues, 0.42f);
        Value[] values = new Value[] {Values.floatArray(vectorValues)};

        SettingsAccessor indexSettings = new IndexSettingObjectMapAccessor(Map.ofEntries(
                entry(IndexSetting.vector_Dimensions(), dimensions),
                entry(IndexSetting.vector_Similarity_Function(), "EUCLIDEAN"),
                entry(IndexSetting.vector_Quantization_Type(), quantizationType.name())));
        VectorIndexConfig config =
                VECTOR_INDEX_VERSION.indexSettingValidator(KERNEL_VERSION).validateToTypedConfig(indexSettings);

        LuceneCodec codec = CONTEXT.codecsFactory().codecFor(config);
        LuceneIndexWriterConfig writerConfig = new LuceneIndexWriterConfig(new KeywordAnalyzer()).setCodec(codec);
        writerConfig.setMergingParameters(1.0, 10, LOG_BYTE_SIZED, 10, 10, 10, 8.0, 10);
        VectorDocumentStructure documentStructure = VectorDocumentStructures.documentStructureFor(VECTOR_INDEX_VERSION);
        LuceneDocumentsFactory documentFactory = CONTEXT.documentsFactory();
        LuceneDirectoryFactory directoryFactory = CONTEXT.directoryFactory();

        try (LuceneDirectory directory = directoryFactory.inMemoryDirectory()) {

            // write a document with vectors
            try (LuceneIndexWriter indexWriter = directory.newWriter(writerConfig)) {
                LuceneDocument vectorDocument = documentFactory.createVectorDocument(
                        documentStructure, 1, (Neo4jVectorSimilarityFunction) config.similarityFunction(), values);
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
