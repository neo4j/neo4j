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
package org.neo4j.kernel.api.impl.index.lucene;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.neo4j.kernel.api.impl.index.lucene.LuceneIndexWriterConfig.MergePolicyOption.LOG_BYTE_SIZED;
import static org.neo4j.logging.LogAssertions.assertThat;

import java.io.IOException;
import java.nio.file.FileVisitResult;
import java.nio.file.FileVisitor;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.List;
import org.apache.lucene.analysis.core.KeywordAnalyzer;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.neo4j.function.ThrowingConsumer;
import org.neo4j.logging.AssertableLogProvider;
import org.neo4j.logging.AssertableLogProvider.Level;
import org.neo4j.test.extension.testdirectory.TestDirectoryExtension;

@TestDirectoryExtension
public class LuceneDirectoryTest {

    @ParameterizedTest
    @EnumSource(LuceneContext.class)
    void shouldLogMergesWhenScheduled(LuceneContext luceneContext) throws IOException, InterruptedException {
        AssertableLogProvider logProvider = new AssertableLogProvider();

        LuceneDirectoryFactory directoryFactory = luceneContext.directoryFactory();
        try (LuceneDirectory directory = directoryFactory.inMemoryDirectory()) {
            runTest(logProvider, luceneContext, directory, iw -> iw.forceMerge(1));
        }

        assertThat(logProvider)
                .forLevel(Level.DEBUG)
                .containsMessagesEventually(500, "starting merge")
                .containsMessagesEventually(500, "merging 2 documents")
                .containsMessagesEventually(500, "finishing merge");
    }

    @TempDir
    Path tmpDir;

    @ParameterizedTest
    @EnumSource(LuceneContext.class)
    void shouldLogMergeErrors(LuceneContext luceneContext) throws IOException, InterruptedException {
        AssertableLogProvider logProvider = new AssertableLogProvider();

        LuceneDirectoryFactory directoryFactory = luceneContext.directoryFactory();
        try (LuceneDirectory directory = directoryFactory.openPersistent(tmpDir)) {

            assertThatThrownBy(() -> runTest(logProvider, luceneContext, directory, iw -> {
                        // remove some index files so that merging will fail
                        FileVisitor<Path> corruptIndex = new SimpleFileVisitor<>() {
                            @Override
                            public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
                                if (file.getFileName().toString().endsWith(".cfs")) {
                                    Files.delete(file);
                                }
                                return FileVisitResult.CONTINUE;
                            }
                        };

                        Files.walkFileTree(tmpDir, corruptIndex);

                        iw.forceMerge(1);
                    }))
                    .hasRootCauseInstanceOf(NoSuchFileException.class);
        }

        assertThat(logProvider).forLevel(Level.ERROR).containsMessagesEventually(500, "failed");
        assertThat(logProvider)
                .forLevel(Level.DEBUG)
                .containsMessagesEventually(500, "starting merge")
                .containsMessagesEventually(500, "finishing merge")
                .doesNotContainMessage("merging 2 documents");
    }

    private static void runTest(
            AssertableLogProvider logProvider,
            LuceneContext luceneContext,
            LuceneDirectory directory,
            ThrowingConsumer<LuceneIndexWriter, ? extends IOException> testBlock)
            throws IOException {

        LuceneIndexWriterConfig indexWriterConfig = new LuceneIndexWriterConfig(new KeywordAnalyzer());
        indexWriterConfig
                .setLogProvider(logProvider)
                .setMergingParameters(
                        1.0, 1.0, LOG_BYTE_SIZED, 32, 32, 1024, 8.0, 10); // parameters to avoid merge during indexing

        try (LuceneIndexWriter indexWriter = directory.newWriter(indexWriterConfig)) {

            LuceneDocumentsFactory documentsFactory = luceneContext.documentsFactory();

            for (String id : List.of("1", "2")) {
                LuceneDocument doc = documentsFactory.newDocument();
                doc.addStringField("id", id, true);
                indexWriter.addDocument(doc);
                indexWriter.commit();
            }

            testBlock.accept(indexWriter);
        }
    }
}
