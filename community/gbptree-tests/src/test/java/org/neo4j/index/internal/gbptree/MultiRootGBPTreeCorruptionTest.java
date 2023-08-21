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

import static org.assertj.core.api.Assertions.assertThat;
import static org.neo4j.index.internal.gbptree.GBPTreeCorruption.pageSpecificCorruption;
import static org.neo4j.index.internal.gbptree.GBPTreeTestUtil.consistencyCheck;
import static org.neo4j.io.IOUtils.closeAll;
import static org.neo4j.io.pagecache.context.CursorContext.NULL_CONTEXT;
import static org.neo4j.io.pagecache.impl.muninn.MuninnPageCache.config;
import static org.neo4j.io.pagecache.tracing.PageCacheTracer.NULL;
import static org.neo4j.memory.EmptyMemoryTracker.INSTANCE;

import java.io.IOException;
import java.nio.file.Path;
import java.util.function.Function;
import org.apache.commons.lang3.mutable.MutableBoolean;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.impl.SingleFilePageSwapperFactory;
import org.neo4j.io.pagecache.impl.muninn.MuninnPageCache;
import org.neo4j.test.RandomSupport;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.RandomExtension;
import org.neo4j.test.extension.testdirectory.EphemeralTestDirectoryExtension;
import org.neo4j.test.scheduler.ThreadPoolJobScheduler;
import org.neo4j.test.utils.TestDirectory;

@ExtendWith(RandomExtension.class)
@EphemeralTestDirectoryExtension
class MultiRootGBPTreeCorruptionTest {
    @Inject
    private FileSystemAbstraction fs;

    @Inject
    private TestDirectory directory;

    @Inject
    private RandomSupport random;

    private ThreadPoolJobScheduler jobScheduler;
    private PageCache pageCache;
    private MultiRootGBPTree<RawBytes, RawBytes, RawBytes> tree;

    private final SimpleByteArrayLayout dataLayout = new SimpleByteArrayLayout();
    private final SimpleByteArrayLayout rootLayout = new SimpleByteArrayLayout();

    @BeforeEach
    void start() {
        jobScheduler = new ThreadPoolJobScheduler();
        pageCache = new MuninnPageCache(
                new SingleFilePageSwapperFactory(fs, NULL, INSTANCE),
                jobScheduler,
                config(10_000).pageSize(256));
        tree = new GBPTreeBuilder<>(pageCache, fs, directory.file("tree"), dataLayout, rootLayout).buildMultiRoot();
    }

    @AfterEach
    void stop() throws IOException {
        closeAll(tree, pageCache, jobScheduler);
    }

    @Test
    void shouldDetectNotATreeNodeLeafInRootTree() throws IOException {
        // given
        rootTreeWithHeight(2);
        var leafNode = random.among(inspect().rootTree().leafNodes());

        // when
        tree.unsafe(pageSpecificCorruption(leafNode, GBPTreeCorruption.notATreeNode()), false, NULL_CONTEXT);

        // then
        assertInconsistency(check -> new GBPTreeConsistencyCheckVisitor.Adaptor() {
            @Override
            public void notATreeNode(long pageId, Path file) {
                check.setTrue();
            }
        });
    }

    @Test
    void shouldDetectNotATreeNodeLeafInDataTree() throws IOException {
        // given
        var rootKey = rootLayout.key(random.nextInt(1_000_000));
        tree.create(rootKey, NULL_CONTEXT);
        var dataTree = tree.access(rootKey);
        dataTreeWithHeight(dataTree, 2);
        var leafNode =
                random.among(inspect().dataTrees().findFirst().orElseThrow().leafNodes());

        // when
        tree.unsafe(pageSpecificCorruption(leafNode, GBPTreeCorruption.notATreeNode()), NULL_CONTEXT);

        // then
        assertInconsistency(check -> new GBPTreeConsistencyCheckVisitor.Adaptor() {
            @Override
            public void notATreeNode(long pageId, Path file) {
                check.setTrue();
            }
        });
    }

    @Test
    void shouldDetectUnknownTreeNodeTypeInRootTree() throws IOException {
        // given
        rootTreeWithHeight(2);
        var rootNode = inspect().rootTree().rootNode();

        // when
        tree.unsafe(pageSpecificCorruption(rootNode, GBPTreeCorruption.unknownTreeNodeType()), true, NULL_CONTEXT);

        // then
        assertInconsistency(check -> new GBPTreeConsistencyCheckVisitor.Adaptor() {
            @Override
            public void unknownTreeNodeType(long pageId, byte treeNodeType, Path file) {
                check.setTrue();
            }
        });
    }

    @Test
    void shouldDetectUnknownTreeNodeTypeInDataTree() throws IOException {
        // given
        var rootKey = rootLayout.key(random.nextInt(1_000_000));
        tree.create(rootKey, NULL_CONTEXT);
        var dataTree = tree.access(rootKey);
        dataTreeWithHeight(dataTree, 2);
        var leafNode =
                random.among(inspect().dataTrees().findFirst().orElseThrow().leafNodes());

        // when
        tree.unsafe(pageSpecificCorruption(leafNode, GBPTreeCorruption.unknownTreeNodeType()), NULL_CONTEXT);

        // then
        assertInconsistency(check -> new GBPTreeConsistencyCheckVisitor.Adaptor() {
            @Override
            public void unknownTreeNodeType(long pageId, byte treeNodeType, Path file) {
                check.setTrue();
            }
        });
    }

    private GBPTreeInspection inspect() throws IOException {
        return tree.visit(new InspectingVisitor<>(), NULL_CONTEXT).get();
    }

    private void rootTreeWithHeight(int height) throws IOException {
        int keyCount = 0;
        do {
            tree.create(rootLayout.key(keyCount), NULL_CONTEXT);
            keyCount++;
        } while (inspect().rootTree().lastLevel() < height);
    }

    private void dataTreeWithHeight(DataTree<RawBytes, RawBytes> tree, int height) throws IOException {
        int keyCount = 0;
        do {
            try (var writer = tree.writer(NULL_CONTEXT)) {
                writer.put(dataLayout.key(keyCount), dataLayout.value(keyCount));
                keyCount++;
            }
        } while (inspect().dataTrees().findFirst().orElseThrow().lastLevel() < height);
    }

    private void assertInconsistency(Function<MutableBoolean, GBPTreeConsistencyCheckVisitor> visitorFunction) {
        var check = new MutableBoolean();
        assertThat(consistencyCheck(tree, visitorFunction.apply(check))).isFalse();
        assertThat(check.booleanValue()).isTrue();
    }
}
