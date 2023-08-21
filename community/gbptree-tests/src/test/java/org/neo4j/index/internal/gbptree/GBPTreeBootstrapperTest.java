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
package org.neo4j.index.internal.gbptree;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.neo4j.index.internal.gbptree.GBPTreeTestUtil.consistencyCheckStrict;
import static org.neo4j.index.internal.gbptree.SimpleLongLayout.longLayout;
import static org.neo4j.io.pagecache.context.CursorContextFactory.NULL_CONTEXT_FACTORY;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.util.stream.Stream;
import org.apache.commons.lang3.mutable.MutableLong;
import org.eclipse.collections.api.factory.Sets;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.neo4j.io.ByteUnit;
import org.neo4j.io.compress.ZipUtils;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.PageCacheOpenOptions;
import org.neo4j.io.pagecache.context.CursorContext;
import org.neo4j.io.pagecache.impl.muninn.StandalonePageCacheFactory;
import org.neo4j.io.pagecache.tracing.FileFlushEvent;
import org.neo4j.io.pagecache.tracing.PageCacheTracer;
import org.neo4j.scheduler.JobScheduler;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.testdirectory.TestDirectoryExtension;
import org.neo4j.test.scheduler.ThreadPoolJobScheduler;
import org.neo4j.test.tags.MultiVersionedTag;
import org.neo4j.test.utils.TestDirectory;

@TestDirectoryExtension
class GBPTreeBootstrapperTest {
    private static final String STORE = "GBPTreeBootstrapperTest_store";
    private static final int PAGE_SIZE_1K = (int) ByteUnit.kibiBytes(1);
    private static final int PAGE_SIZE_8K = (int) ByteUnit.kibiBytes(8);
    private static final int PAGE_SIZE_16K = (int) ByteUnit.kibiBytes(16);
    private static final int PAGE_SIZE_32K = (int) ByteUnit.kibiBytes(32);
    private static final int PAGE_SIZE_64K = (int) ByteUnit.kibiBytes(64);
    private static final int PAGE_SIZE_4M = (int) ByteUnit.mebiBytes(4);
    private static final String ZIP_NAME_1k = "GBPTreeBootstrapperTest_store_1k.zip";
    private static final String ZIP_NAME_8k = "GBPTreeBootstrapperTest_store_8k.zip";
    private static final String ZIP_NAME_16k = "GBPTreeBootstrapperTest_store_16k.zip";
    private static final String ZIP_NAME_32k = "GBPTreeBootstrapperTest_store_32k.zip";
    private static final String ZIP_NAME_64k = "GBPTreeBootstrapperTest_store_64k.zip";
    private static final String ZIP_NAME_4M = "GBPTreeBootstrapperTest_store_4M.zip";

    @Inject
    TestDirectory dir;

    @Inject
    FileSystemAbstraction fs;

    private SimpleLongLayout layout;
    private JobScheduler scheduler;
    private PageCache pageCache;
    private String zipName;
    private Path storeFile;
    private Path zipFile;

    @AfterEach
    void tearDown() throws Exception {
        if (pageCache != null) {
            pageCache.close();
            pageCache = null;
        }
        if (scheduler != null) {
            scheduler.close();
            scheduler = null;
        }
    }

    @Disabled("Example showing how test files where created")
    @ParameterizedTest
    @MethodSource("testSetupStream")
    void createTreeFilesWithDifferentPageSizes(TestSetup testSetup) throws IOException {
        setupTest(testSetup);

        try (GBPTree<MutableLong, MutableLong> tree = new GBPTreeBuilder<>(pageCache, fs, storeFile, layout)
                .with(Sets.immutable.of(PageCacheOpenOptions.BIG_ENDIAN))
                .build()) {
            tree.checkpoint(FileFlushEvent.NULL, CursorContext.NULL_CONTEXT);
        }
        ZipUtils.zip(dir.getFileSystem(), storeFile, zipFile);
        fail(String.format(
                "Zip file created with store. Copy to correct resource using:%nmv \"%s\" \"%s\"",
                zipFile.toAbsolutePath(),
                "<corresponding-module>" + pathify(".src.test.resources.")
                        + pathify(getClass().getPackage().getName() + ".") + zipName));
    }

    @ParameterizedTest
    @MethodSource("testSetupStream")
    @MultiVersionedTag
    void shouldBootstrapTreeOfDifferentPageSizes(TestSetup testSetup) throws Exception {
        setupTest(testSetup);

        ZipUtils.unzipResource(getClass(), zipName, storeFile);

        LayoutBootstrapper layoutBootstrapper =
                meta -> new LayoutBootstrapper.Layouts(layout, RootLayerConfiguration.singleRoot());
        try (JobScheduler scheduler = new ThreadPoolJobScheduler();
                GBPTreeBootstrapper bootstrapper = new GBPTreeBootstrapper(
                        fs, scheduler, layoutBootstrapper, true, NULL_CONTEXT_FACTORY, PageCacheTracer.NULL)) {
            GBPTreeBootstrapper.Bootstrap bootstrap =
                    bootstrapper.bootstrapTree(storeFile, PageCacheOpenOptions.BIG_ENDIAN);
            assertTrue(bootstrap.isTree());
            try (MultiRootGBPTree<?, ?, ?> tree = bootstrap.tree()) {
                consistencyCheckStrict(tree);
            }
        }
    }

    private void setupTest(TestSetup testSetup) {
        this.layout = testSetup.layout;
        this.scheduler = new ThreadPoolJobScheduler();
        this.pageCache = StandalonePageCacheFactory.createPageCache(fs, scheduler, testSetup.pageSize);
        this.zipName = testSetup.zipName;
        this.storeFile = dir.file(STORE);
        this.zipFile = dir.file(zipName);
    }

    private static String pathify(String name) {
        return name.replace('.', File.separatorChar);
    }

    public static Stream<TestSetup> testSetupStream() {
        return Stream.of(
                new TestSetup(PAGE_SIZE_1K, ZIP_NAME_1k, longLayout().build()),
                new TestSetup(PAGE_SIZE_8K, ZIP_NAME_8k, longLayout().build()),
                new TestSetup(PAGE_SIZE_16K, ZIP_NAME_16k, longLayout().build()),
                new TestSetup(PAGE_SIZE_32K, ZIP_NAME_32k, longLayout().build()),
                new TestSetup(PAGE_SIZE_64K, ZIP_NAME_64k, longLayout().build()),
                new TestSetup(PAGE_SIZE_4M, ZIP_NAME_4M, longLayout().build()));
    }

    private static class TestSetup {
        private final int pageSize;
        private final String zipName;
        private final SimpleLongLayout layout;

        TestSetup(int pageSize, String zipName, SimpleLongLayout layout) {
            this.pageSize = pageSize;
            this.zipName = zipName;
            this.layout = layout;
        }
    }
}
