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

import static org.neo4j.index.internal.gbptree.CursorCreator.bind;
import static org.neo4j.index.internal.gbptree.Generation.stableGeneration;
import static org.neo4j.index.internal.gbptree.Generation.unstableGeneration;
import static org.neo4j.index.internal.gbptree.SeekCursor.LEAF_LEVEL;
import static org.neo4j.index.internal.gbptree.TreeNodeUtil.DATA_LAYER_FLAG;
import static org.neo4j.io.pagecache.PagedFile.PF_SHARED_READ_LOCK;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.List;
import org.neo4j.common.DependencyResolver;
import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.io.pagecache.context.CursorContext;
import org.neo4j.io.pagecache.context.CursorContextFactory;
import org.neo4j.util.Preconditions;

class SingleRootLayer<KEY, VALUE> extends RootLayer<SingleRoot, KEY, VALUE> {
    private final Layout<KEY, VALUE> layout;
    private final LeafNodeBehaviour<KEY, VALUE> leafNode;
    private final InternalNodeBehaviour<KEY> internalNode;
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
        this.leafNode = format.createLeafBehaviour(support.payloadSize(), layout, offloadStore, dependencyResolver);
        this.internalNode =
                format.createInternalBehaviour(support.payloadSize(), layout, offloadStore, dependencyResolver);
        this.singleRootAccess = new SingleDataTree();
    }

    @Override
    public void initializeAfterCreation(Root firstRoot, CursorContext cursorContext) throws IOException {
        setRoot(firstRoot, cursorContext);
        support.writeMeta(null, layout, cursorContext, treeNodeSelector);
        support.initializeNewRoot(root, leafNode, DATA_LAYER_FLAG, cursorContext);
    }

    @Override
    void initialize(Root root, CursorContext cursorContext) throws IOException {
        setRoot(root, cursorContext);
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
                    null,
                    null,
                    null,
                    layout,
                    leafNode,
                    internalNode,
                    stableGeneration(generation),
                    unstableGeneration(generation));
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
                        leafNode,
                        internalNode,
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
                .check(visitor, state.progress, GBPTreeConsistencyChecker.NO_MONITOR);
    }

    @Override
    public int keyValueSizeCap() {
        return leafNode.keyValueSizeCap();
    }

    @Override
    public int inlineKeyValueSizeCap() {
        return leafNode.inlineKeyValueSizeCap();
    }

    @Override
    int leafNodeMaxKeys() {
        return leafNode.maxKeyCount();
    }

    @Override
    void visitAllDataTreeRoots(CursorContext cursorContext, TreeRootsVisitor<SingleRoot> visitor) {
        visitor.accept(SingleRoot.SINGLE_ROOT);
    }

    @Override
    void unsafe(GBPTreeUnsafe unsafe, boolean dataTree, CursorContext cursorContext) throws IOException {
        Preconditions.checkState(dataTree, "Can only operate on data tree");
        support.unsafe(unsafe, layout, leafNode, internalNode, cursorContext);
    }

    @Override
    CrashGenerationCleaner createCrashGenerationCleaner(CursorContextFactory contextFactory) {
        return support.createCrashGenerationCleaner(null, internalNode, contextFactory);
    }

    @Override
    void printNode(PageCursor cursor, CursorContext cursorContext) {
        try {
            long generation = support.generation();
            long stableGeneration = stableGeneration(generation);
            long unstableGeneration = unstableGeneration(generation);
            new GBPTreeStructure<>(
                            null, null, null, layout, leafNode, internalNode, stableGeneration, unstableGeneration)
                    .visitTreeNode(cursor, new PrintingGBPTreeVisitor<>(PrintConfig.defaults()), cursorContext);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private class SingleDataTree implements DataTree<KEY, VALUE> {
        private final GBPTreeWriter<KEY, VALUE> batchedWriter;

        SingleDataTree() {
            this.batchedWriter = support.newWriter(
                    layout,
                    SingleRootLayer.this,
                    leafNode,
                    internalNode,
                    TreeWriterCoordination.NO_COORDINATION,
                    false,
                    DATA_LAYER_FLAG);
        }

        @Override
        public Seeker<KEY, VALUE> allocateSeeker(CursorContext cursorContext) throws IOException {
            return support.internalAllocateSeeker(layout, cursorContext, leafNode, internalNode);
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
                    LEAF_LEVEL,
                    SeekCursor.NO_MONITOR);
        }

        @Override
        public List<KEY> partitionedSeek(
                KEY fromInclusive, KEY toExclusive, int numberOfPartitions, CursorContext cursorContext)
                throws IOException {
            return support.internalPartitionedSeek(
                    layout,
                    leafNode,
                    internalNode,
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
                        layout,
                        leafNode,
                        internalNode,
                        splitRatio,
                        cursorContext,
                        SingleRootLayer.this,
                        DATA_LAYER_FLAG);
            }
        }

        @Override
        public long estimateNumberOfEntriesInTree(CursorContext cursorContext) throws IOException {
            return support.estimateNumberOfEntriesInTree(
                    layout, leafNode, internalNode, SingleRootLayer.this, cursorContext);
        }
    }
}
