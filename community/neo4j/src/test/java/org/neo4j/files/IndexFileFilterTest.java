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
package org.neo4j.files;

import static org.assertj.core.api.Assertions.assertThat;
import static org.neo4j.kernel.api.index.IndexDirectoryStructure.directoriesByProvider;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestInstance.Lifecycle;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.neo4j.internal.schema.IndexProviderDescriptor;
import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.kernel.api.impl.schema.TextIndexProvider;
import org.neo4j.kernel.api.impl.schema.trigram.TrigramIndexProvider;
import org.neo4j.kernel.api.impl.schema.vector.VectorIndexVersion;
import org.neo4j.kernel.impl.index.schema.FulltextIndexProviderFactory;
import org.neo4j.kernel.impl.index.schema.PointIndexProvider;
import org.neo4j.kernel.impl.index.schema.RangeIndexProvider;
import org.neo4j.kernel.impl.index.schema.TokenIndexProvider;
import org.neo4j.kernel.internal.IndexFileFilter;
import org.neo4j.kernel.internal.LuceneIndexFileFilter;
import org.neo4j.kernel.internal.NativeIndexFileFilter;
import org.neo4j.test.RandomSupport;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.RandomExtension;
import org.neo4j.test.extension.testdirectory.TestDirectoryExtension;
import org.neo4j.test.utils.TestDirectory;

class IndexFileFilterTest {
    private static final List<IndexProviderDescriptor> NATIVE =
            List.of(TokenIndexProvider.DESCRIPTOR, RangeIndexProvider.DESCRIPTOR, PointIndexProvider.DESCRIPTOR);

    private static final List<IndexProviderDescriptor> LUCENE = List.of(
            TextIndexProvider.DESCRIPTOR,
            TrigramIndexProvider.DESCRIPTOR,
            FulltextIndexProviderFactory.DESCRIPTOR,
            VectorIndexVersion.V1_0.descriptor());

    @TestDirectoryExtension
    @ExtendWith(RandomExtension.class)
    @TestInstance(Lifecycle.PER_CLASS)
    abstract static class IndexFileFilterTestBase {
        @Inject
        private DefaultFileSystemAbstraction fs;

        @Inject
        private TestDirectory directory;

        @Inject
        private RandomSupport random;

        private Path storeDir;
        private IndexFileFilter filter;

        @BeforeAll
        void before() {
            storeDir = directory.homePath();
            filter = indexFileFilter(storeDir);
        }

        @AfterEach
        void after() throws IOException {
            directory.cleanup();
        }

        abstract IndexFileFilter indexFileFilter(Path storeDir);

        abstract Iterable<IndexProviderDescriptor> shouldAcceptIndexFile();

        abstract Iterable<IndexProviderDescriptor> shouldNotAcceptIndexFile();

        @ParameterizedTest
        @MethodSource
        void shouldAcceptIndexFile(IndexProviderDescriptor descriptor) throws IOException {
            final var indexDirectory =
                    directoriesByProvider(storeDir).forProvider(descriptor).directoryForIndex(1);
            final var file = createRandomFileFrom(indexDirectory);
            assertThat(filter).as("expected to accept index file").accepts(file);
        }

        @ParameterizedTest
        @MethodSource
        void shouldNotAcceptIndexFile(IndexProviderDescriptor descriptor) throws IOException {
            final var indexDirectory =
                    directoriesByProvider(storeDir).forProvider(descriptor).directoryForIndex(1);
            final var file = createRandomFileFrom(indexDirectory);
            assertThat(filter).as("expected to reject index file").rejects(file);
        }

        private Path createRandomFileFrom(Path root) throws IOException {
            final var depth = random.nextInt(1, 5);
            var file = root;
            for (int i = 0; i < depth; i++) {
                file = file.resolve(random.nextAlphaNumericString());
            }

            fs.mkdirs(file.getParent());
            try (final var channel = fs.write(file)) {}
            return file;
        }
    }

    @Nested
    class NativeIndexFileFilterTest extends IndexFileFilterTestBase {
        @Override
        IndexFileFilter indexFileFilter(Path storeDir) {
            return new NativeIndexFileFilter(storeDir);
        }

        @Override
        Iterable<IndexProviderDescriptor> shouldAcceptIndexFile() {
            return NATIVE;
        }

        @Override
        Iterable<IndexProviderDescriptor> shouldNotAcceptIndexFile() {
            return LUCENE;
        }
    }

    @Nested
    class LuceneIndexFileFilterTest extends IndexFileFilterTestBase {
        @Override
        IndexFileFilter indexFileFilter(Path storeDir) {
            return new LuceneIndexFileFilter(storeDir);
        }

        @Override
        Iterable<IndexProviderDescriptor> shouldAcceptIndexFile() {
            return LUCENE;
        }

        @Override
        Iterable<IndexProviderDescriptor> shouldNotAcceptIndexFile() {
            return NATIVE;
        }
    }
}
