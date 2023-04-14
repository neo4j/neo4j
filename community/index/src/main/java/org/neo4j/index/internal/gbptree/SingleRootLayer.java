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

import static org.neo4j.index.internal.gbptree.CursorCreator.bind;
import static org.neo4j.index.internal.gbptree.Generation.stableGeneration;
import static org.neo4j.index.internal.gbptree.Generation.unstableGeneration;
import static org.neo4j.index.internal.gbptree.SeekCursor.LEAF_LEVEL;
import static org.neo4j.index.internal.gbptree.TreeNodeUtil.DATA_LAYER_FLAG;
import static org.neo4j.io.pagecache.PagedFile.PF_SHARED_READ_LOCK;

import java.io.IOException;
import java.util.List;
import org.neo4j.common.DependencyResolver;
import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.io.pagecache.context.CursorContext;
import org.neo4j.io.pagecache.context.CursorContextFactory;
import org.neo4j.util.Preconditions;

class SingleRootLayer<KEY, VALUE> extends RootLayer<SingleRoot, KEY, VALUE> {
    private final Layout<KEY, VALUE> layout;
    private final TreeNode<KEY, VALUE> treeNode;
    private final SingleDataTree singleRootAccess;

    SingleRootLayer(
            RootLayerSupport support,
            Layout<KEY, VALUE> layout,
            TreeNodeSelector treeNodeSelector,
            DependencyResolver dependencyResolver) {
        super(support, treeNodeSelector);
        this.layout = layout;

        var format = treeNodeSelector.selectByLayout(layout);
        OffloadStoreImpl<KEY, VALUE> offloadStore = support.buildOffload(layout);
        this.treeNode = format.create(support.payloadSize(), layout, offloadStore, dependencyResolver);
        this.singleRootAccess = new SingleDataTree();
    }

    @Override
    public void initializeAfterCreation(Root firstRoot, CursorContext cursorContext) throws IOException {
        setRoot(firstRoot);
        support.writeMeta(null, layout, cursorContext, treeNodeSelector);
        support.initializeNewRoot(root, treeNode, DATA_LAYER_FLAG, cursorContext);
    }

    @Override
    void initialize(Root root, CursorContext cursorContext) throws IOException {
        setRoot(root);
        support.readMeta(cursorContext).verify(layout, null, treeNodeSelector);
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
                    null, null, treeNode, layout, stableGeneration(generation), unstableGeneration(generation));
            structure.visitTree(cursor, visitor, cursorContext);
            support.idProvider().visitFreelist(visitor, bind(support, PF_SHARED_READ_LOCK, cursorContext));
        }
    }

    @Override
    public void consistencyCheck(
            GBPTreeConsistencyChecker.ConsistencyCheckState state,
            GBPTreeConsistencyCheckVisitor visitor,
            boolean reportDirty,
            CursorContextFactory contextFactory,
            int numThreads)
            throws IOException {
        long generation = support.generation();
        var pagedFile = support.pagedFile();
        new GBPTreeConsistencyChecker<>(
                        treeNode,
                        layout,
                        state,
                        numThreads,
                        stableGeneration(generation),
                        unstableGeneration(generation),
                        reportDirty,
                        pagedFile.path(),
                        ctx -> pagedFile.io(0, PF_SHARED_READ_LOCK, ctx),
                        root,
                        contextFactory)
                .check(visitor, state.progress);
    }

    @Override
    public int keyValueSizeCap() {
        return treeNode.keyValueSizeCap();
    }

    @Override
    public int inlineKeyValueSizeCap() {
        return treeNode.inlineKeyValueSizeCap();
    }

    @Override
    void visitAllDataTreeRoots(CursorContext cursorContext, TreeRootsVisitor<SingleRoot> visitor) {
        visitor.accept(SingleRoot.SINGLE_ROOT);
    }

    @Override
    void unsafe(GBPTreeUnsafe unsafe, boolean dataTree, CursorContext cursorContext) throws IOException {
        Preconditions.checkState(dataTree, "Can only operate on data tree");
        support.unsafe(unsafe, layout, treeNode, cursorContext);
    }

    @Override
    CrashGenerationCleaner createCrashGenerationCleaner(CursorContextFactory contextFactory) {
        return support.createCrashGenerationCleaner(null, treeNode, contextFactory);
    }

    @Override
    void printNode(PageCursor cursor, CursorContext cursorContext) {
        long generation = support.generation();
        treeNode.printNode(
                cursor, false, true, stableGeneration(generation), unstableGeneration(generation), cursorContext);
    }

    private class SingleDataTree implements DataTree<KEY, VALUE> {
        private final GBPTreeWriter<KEY, VALUE> batchedWriter;

        SingleDataTree() {
            this.batchedWriter = support.newWriter(
                    layout,
                    SingleRootLayer.this,
                    treeNode,
                    TreeWriterCoordination.NO_COORDINATION,
                    false,
                    DATA_LAYER_FLAG);
        }

        @Override
        public Seeker<KEY, VALUE> allocateSeeker(CursorContext cursorContext) throws IOException {
            return support.internalAllocateSeeker(layout, treeNode, cursorContext, SeekCursor.NO_MONITOR);
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
                    treeNode,
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
                        layout, treeNode, splitRatio, cursorContext, SingleRootLayer.this, DATA_LAYER_FLAG);
            }
        }

        @Override
        public long estimateNumberOfEntriesInTree(CursorContext cursorContext) throws IOException {
            return support.estimateNumberOfEntriesInTree(layout, treeNode, SingleRootLayer.this, cursorContext);
        }
    }
}
