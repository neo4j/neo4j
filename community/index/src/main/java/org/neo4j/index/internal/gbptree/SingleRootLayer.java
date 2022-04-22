/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
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
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.index.internal.gbptree;

import static org.neo4j.index.internal.gbptree.Generation.stableGeneration;
import static org.neo4j.index.internal.gbptree.Generation.unstableGeneration;
import static org.neo4j.index.internal.gbptree.SeekCursor.LEAF_LEVEL;
import static org.neo4j.index.internal.gbptree.TreeNode.DATA_LAYER_FLAG;
import static org.neo4j.io.pagecache.PagedFile.PF_SHARED_READ_LOCK;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;
import java.util.function.Consumer;
import java.util.function.Supplier;
import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.io.pagecache.context.CursorContext;
import org.neo4j.io.pagecache.context.CursorContextFactory;
import org.neo4j.util.Preconditions;

class SingleRootLayer<KEY, VALUE> extends RootLayer<SingleRoot, KEY, VALUE> {
    private final RootLayerSupport support;
    private final Layout<KEY, VALUE> layout;
    private final TreeNode<KEY, VALUE> singleTreeNode;
    private final Supplier<TreeNode<KEY, VALUE>> treeNodeFactory;
    private final OffloadStoreImpl<KEY, VALUE> offloadStore;
    private final SingleDataTree singleRootAccess;
    private volatile Root root;

    SingleRootLayer(RootLayerSupport support, Layout<KEY, VALUE> layout, boolean created, CursorContext cursorContext)
            throws IOException {
        this.support = support;
        this.layout = layout;

        if (created) {
            support.writeMeta(null, layout, cursorContext);
        } else {
            support.readMeta(cursorContext).verify(layout, (Layout<?, ?>) null);
        }
        TreeNodeSelector.Factory format = TreeNodeSelector.selectByLayout(layout);
        this.offloadStore = support.buildOffload(layout);
        this.treeNodeFactory = () -> format.create(support.payloadSize(), layout, offloadStore);
        this.singleTreeNode = treeNodeFactory.get();
        this.singleRootAccess = new SingleDataTree();
    }

    @Override
    public Root getRoot() {
        return root;
    }

    @Override
    public void setRoot(Root root) throws IOException {
        this.root = root;
    }

    @Override
    public void initializeAfterCreation(CursorContext cursorContext) throws IOException {
        support.initializeNewRoot(root, singleTreeNode, DATA_LAYER_FLAG, cursorContext);
    }

    @Override
    public void create(SingleRoot dataRootKey, CursorContext cursorContext) {
        singleRootException();
    }

    @Override
    public void delete(SingleRoot dataRootKey, CursorContext cursorContext) {
        singleRootException();
    }

    private void singleRootException() {
        throw new UnsupportedOperationException(
                "This is a single-data-tree GBPTree, which means it always has one data tree which cannot be deleted and no more data trees can be created");
    }

    @Override
    public DataTree<KEY, VALUE> access(SingleRoot dataRootKey) {
        return singleRootAccess;
    }

    @Override
    public void visit(GBPTreeVisitor<SingleRoot, KEY, VALUE> visitor, CursorContext cursorContext) throws IOException {
        try (PageCursor cursor = support.openRootCursor(root, PF_SHARED_READ_LOCK, cursorContext)) {
            long generation = support.generation();
            GBPTreeStructure<SingleRoot, KEY, VALUE> structure = new GBPTreeStructure<>(
                    null, null, singleTreeNode, layout, stableGeneration(generation), unstableGeneration(generation));
            structure.visitTree(cursor, visitor, cursorContext);
            support.idProvider().visitFreelist(visitor, cursorContext);
        }
    }

    @Override
    public void consistencyCheck(
            GBPTreeConsistencyChecker.ConsistencyCheckState state,
            GBPTreeConsistencyCheckVisitor visitor,
            boolean reportDirty,
            PageCursor cursor,
            CursorContext cursorContext,
            Path file)
            throws IOException {
        long generation = support.generation();
        new GBPTreeConsistencyChecker<>(
                        singleTreeNode,
                        layout,
                        state,
                        stableGeneration(generation),
                        unstableGeneration(generation),
                        reportDirty)
                .check(file, cursor, root, visitor, cursorContext);
    }

    @Override
    public int keyValueSizeCap() {
        return singleTreeNode.keyValueSizeCap();
    }

    @Override
    public int inlineKeyValueSizeCap() {
        return singleTreeNode.inlineKeyValueSizeCap();
    }

    @Override
    void visitAllDataTreeRoots(Consumer<SingleRoot> visitor, CursorContext cursorContext) throws IOException {
        visitor.accept(SingleRoot.SINGLE_ROOT);
    }

    @Override
    void unsafe(GBPTreeUnsafe unsafe, boolean dataTree, CursorContext cursorContext) throws IOException {
        Preconditions.checkState(dataTree, "Can only operate on data tree");
        support.unsafe(unsafe, layout, singleTreeNode, cursorContext);
    }

    @Override
    CrashGenerationCleaner createCrashGenerationCleaner(CursorContextFactory contextFactory) {
        return support.createCrashGenerationCleaner(null, singleTreeNode, contextFactory);
    }

    @Override
    void printNode(PageCursor cursor, CursorContext cursorContext) {
        long generation = support.generation();
        singleTreeNode.printNode(
                cursor, false, true, stableGeneration(generation), unstableGeneration(generation), cursorContext);
    }

    private class SingleDataTree implements DataTree<KEY, VALUE> {
        private final GBPTreeWriter<KEY, VALUE> batchedWriter;

        SingleDataTree() {
            this.batchedWriter = support.newWriter(
                    layout,
                    SingleRootLayer.this,
                    singleTreeNode,
                    TreeWriterCoordination.NO_COORDINATION,
                    false,
                    DATA_LAYER_FLAG);
        }

        @Override
        public Seeker<KEY, VALUE> allocateSeeker(CursorContext cursorContext) throws IOException {
            return support.internalAllocateSeeker(layout, singleTreeNode, cursorContext, SeekCursor.NO_MONITOR);
        }

        @Override
        public Seeker<KEY, VALUE> seek(Seeker<KEY, VALUE> seeker, KEY fromInclusive, KEY toExclusive)
                throws IOException {
            return support.initializeSeeker(
                    seeker,
                    SingleRootLayer.this,
                    fromInclusive,
                    toExclusive,
                    SeekCursor.DEFAULT_MAX_READ_AHEAD,
                    LEAF_LEVEL);
        }

        @Override
        public List<KEY> partitionedSeek(
                KEY fromInclusive, KEY toExclusive, int numberOfPartitions, CursorContext cursorContext)
                throws IOException {
            return support.internalPartitionedSeek(
                    layout,
                    singleTreeNode,
                    fromInclusive,
                    toExclusive,
                    numberOfPartitions,
                    SingleRootLayer.this,
                    cursorContext);
        }

        @Override
        public Writer<KEY, VALUE> writer(int flags, CursorContext cursorContext) throws IOException {
            double splitRatio = splitRatio(flags);
            if ((flags & DataTree.W_BATCHED_SINGLE_THREADED) != 0) {
                return support.initializeWriter(batchedWriter, splitRatio, cursorContext);
            } else {
                return support.internalParallelWriter(
                        layout, treeNodeFactory, splitRatio, cursorContext, SingleRootLayer.this, DATA_LAYER_FLAG);
            }
        }

        @Override
        public long estimateNumberOfEntriesInTree(CursorContext cursorContext) throws IOException {
            return support.estimateNumberOfEntriesInTree(layout, singleTreeNode, SingleRootLayer.this, cursorContext);
        }
    }
}
