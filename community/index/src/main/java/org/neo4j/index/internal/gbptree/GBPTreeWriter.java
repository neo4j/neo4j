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

import static java.lang.String.format;
import static org.neo4j.index.internal.gbptree.Generation.stableGeneration;
import static org.neo4j.index.internal.gbptree.Generation.unstableGeneration;
import static org.neo4j.index.internal.gbptree.PointerChecking.assertNoSuccessor;
import static org.neo4j.index.internal.gbptree.PointerChecking.checkOutOfBounds;
import static org.neo4j.index.internal.gbptree.TreeNode.generation;
import static org.neo4j.index.internal.gbptree.TreeNode.isInternal;
import static org.neo4j.index.internal.gbptree.TreeNode.keyCount;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.function.Consumer;
import java.util.function.LongSupplier;
import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.io.pagecache.PageCursorUtil;
import org.neo4j.io.pagecache.PagedFile;
import org.neo4j.io.pagecache.context.CursorContext;

class GBPTreeWriter<K, V> implements Writer<K, V> {
    private final InternalTreeLogic<K, V> treeLogic;
    private final ReadWriteLock checkpointLock;
    private final ReadWriteLock writerLock;
    private final FreeListIdProvider freeList;
    private final GBPTree.Monitor monitor;
    private final Consumer<Throwable> exceptionMessageAppender;
    private final LongSupplier generationSupplier;
    private final StructurePropagation<K> structurePropagation;
    private final PagedFile pagedFile;
    private final TreeWriterCoordination coordination;
    private final TreeNode<K, V> bTreeNode;
    private final boolean parallel;
    private final TreeRootExchange rootExchange;
    private final Layout<K, V> layout;
    private boolean writerLockAcquired;
    private PageCursor cursor;
    private CursorContext cursorContext;
    private double ratioToKeepInLeftOnSplit;
    private Root root;

    // Writer can't live past a checkpoint because of the mutex with checkpoint,
    // therefore safe to locally cache these generation fields from the volatile generation in the tree
    private long stableGeneration;
    private long unstableGeneration;

    GBPTreeWriter(
            Layout<K, V> layout,
            PagedFile pagedFile,
            TreeWriterCoordination coordination,
            InternalTreeLogic<K, V> treeLogic,
            TreeNode<K, V> bTreeNode,
            boolean parallel,
            TreeRootExchange rootExchange,
            ReadWriteLock checkpointLock,
            ReadWriteLock writerLock,
            FreeListIdProvider freeList,
            GBPTree.Monitor monitor,
            Consumer<Throwable> exceptionMessageAppender,
            LongSupplier generationSupplier) {
        this.layout = layout;
        this.pagedFile = pagedFile;
        this.coordination = coordination;
        this.bTreeNode = bTreeNode;
        this.parallel = parallel;
        this.rootExchange = rootExchange;
        this.structurePropagation = new StructurePropagation<>(layout.newKey(), layout.newKey(), layout.newKey());
        this.treeLogic = treeLogic;
        this.checkpointLock = checkpointLock;
        this.writerLock = writerLock;
        this.freeList = freeList;
        this.monitor = monitor;
        this.exceptionMessageAppender = exceptionMessageAppender;
        this.generationSupplier = generationSupplier;
    }

    /**
     * When leaving initialize, writer should be in a fully consistent state.
     * <p>
     * Either fully initialized:
     * <ul>
     *    <li>{@link #checkpointLock} - acquired read lock</li>
     *    <li>{@link #writerLock} - acquired read/write lock depending on parallel or not</li>
     *    <li>{@link #cursor} - not null</li>
     * </ul>
     * Of fully closed:
     * <ul>
     *    <li>{@link #checkpointLock} - released read lock</li>
     *    <li>{@link #writerLockAcquired} - released read/write lock depending on parallel or not</li>
     *    <li>{@link #cursor} - null</li>
     * </ul>
     *
     * @param ratioToKeepInLeftOnSplit Decide how much to keep in left node on split, 0=keep nothing, 0.5=split 50-50, 1=keep everything.
     * @param cursorContext underlying page cursor context
     * @throws IOException if fail to open {@link PageCursor}
     */
    void initialize(double ratioToKeepInLeftOnSplit, CursorContext cursorContext) throws IOException {
        if (writerLockAcquired) {
            throw appendTreeInformation(
                    new IllegalStateException(format("This writer has already been initialized %s", this)));
        }
        acquireLockForWriter();

        boolean success = false;
        try {
            writerLockAcquired = true;
            cursor = pagedFile.io(0L /*Ignored*/, PagedFile.PF_SHARED_WRITE_LOCK, cursorContext);
            this.cursorContext = cursorContext;
            long generation = generationSupplier.getAsLong();
            stableGeneration = stableGeneration(generation);
            unstableGeneration = unstableGeneration(generation);
            this.ratioToKeepInLeftOnSplit = ratioToKeepInLeftOnSplit;
            root = rootExchange.getRoot();
            if (!coordination.mustStartFromRoot()) {
                root.goTo(cursor);
                assert assertNoSuccessor(cursor, stableGeneration, unstableGeneration);
                treeLogic.initialize(cursor, ratioToKeepInLeftOnSplit);
            }
            success = true;
        } catch (Throwable e) {
            exceptionMessageAppender.accept(e);
            throw e;
        } finally {
            if (!success) {
                close();
            }
        }
    }

    private void acquireLockForWriter() {
        checkpointLock.readLock().lock();
        try {
            if (parallel) {
                if (!writerLock.readLock().tryLock()) {
                    throw appendTreeInformation(new IllegalStateException(
                            "Single writer from GBPTree#writer() is active and cannot co-exist with parallel writers"));
                }
            } else {
                if (!writerLock.writeLock().tryLock()) {
                    throw appendTreeInformation(
                            new IllegalStateException(
                                    "Single writer from GBPTree#writer() is already acquired by someone else or one or more parallel writers are active"));
                }
            }
        } catch (Throwable t) {
            checkpointLock.readLock().unlock();
            throw t;
        }
    }

    private <T extends Exception> T appendTreeInformation(T exception) {
        exceptionMessageAppender.accept(exception);
        return exception;
    }

    @Override
    public void put(K key, V value) {
        merge(key, value, ValueMergers.overwrite());
    }

    @Override
    public void merge(K key, V value, ValueMerger<K, V> valueMerger) {
        internalMerge(key, value, valueMerger, true);
    }

    @Override
    public void mergeIfExists(K key, V value, ValueMerger<K, V> valueMerger) {
        internalMerge(key, value, valueMerger, false);
    }

    private void internalMerge(K key, V value, ValueMerger<K, V> valueMerger, boolean createIfNotExists) {
        try {
            // Try optimistic mode first
            coordination.initialize();
            if (!goToRoot()
                    || !treeLogic.insert(
                            cursor,
                            structurePropagation,
                            key,
                            value,
                            valueMerger,
                            createIfNotExists,
                            stableGeneration,
                            unstableGeneration,
                            cursorContext)) {
                // OK, didn't work. Flip to pessimistic mode and try again.
                coordination.flipToPessimisticMode();
                assert structurePropagation.isEmpty();
                if (!goToRoot()
                        || !treeLogic.insert(
                                cursor,
                                structurePropagation,
                                key,
                                value,
                                valueMerger,
                                createIfNotExists,
                                stableGeneration,
                                unstableGeneration,
                                cursorContext)) {
                    throw appendTreeInformation(new TreeInconsistencyException(
                            "Unable to insert key:%s value:%s in pessimistic mode", key, value));
                }
            }

            handleStructureChanges(cursorContext);
        } catch (IOException e) {
            exceptionMessageAppender.accept(e);
            throw new UncheckedIOException(e);
        } catch (Throwable t) {
            exceptionMessageAppender.accept(t);
            throw t;
        } finally {
            coordination.reset();
        }

        checkOutOfBounds(cursor);
    }

    private boolean goToRoot() throws IOException {
        if (!coordination.mustStartFromRoot()) {
            return true;
        }

        while (true) {
            coordination.beforeTraversingToChild(root.id(), 0);
            // check again, after locked
            Root rootAfterLock = rootExchange.getRoot();
            if (!rootAfterLock.equals(root)) {
                // There was a root change in between getting the root id and locking it
                coordination.reset();
                root = rootAfterLock;
            } else {
                TreeNode.goTo(cursor, "Root", root.id());
                break;
            }
        }

        assert assertNoSuccessor(cursor, stableGeneration, unstableGeneration);
        treeLogic.initialize(cursor, ratioToKeepInLeftOnSplit);
        int keyCount = keyCount(cursor);
        return coordination.arrivedAtChild(
                isInternal(cursor),
                bTreeNode.availableSpace(cursor, keyCount),
                generation(cursor) != unstableGeneration,
                keyCount);
    }

    private void setRoot(long rootPointer) throws IOException {
        long rootId = GenerationSafePointerPair.pointer(rootPointer);
        rootExchange.setRoot(new Root(rootId, unstableGeneration));
        treeLogic.initialize(cursor, ratioToKeepInLeftOnSplit);
    }

    @Override
    public V remove(K key) {
        V removedValue = layout.newValue();
        InternalTreeLogic.RemoveResult result;
        try {
            // Try optimistic mode
            coordination.initialize();
            if (!goToRoot()
                    || (result = treeLogic.remove(
                                    cursor,
                                    structurePropagation,
                                    key,
                                    removedValue,
                                    stableGeneration,
                                    unstableGeneration,
                                    cursorContext))
                            == InternalTreeLogic.RemoveResult.FAIL) {
                // OK, didn't work. Flip to pessimistic mode and try again.
                coordination.flipToPessimisticMode();
                assert structurePropagation.isEmpty();
                if (!goToRoot()
                        || (result = treeLogic.remove(
                                        cursor,
                                        structurePropagation,
                                        key,
                                        removedValue,
                                        stableGeneration,
                                        unstableGeneration,
                                        cursorContext))
                                == InternalTreeLogic.RemoveResult.FAIL) {
                    throw appendTreeInformation(
                            new TreeInconsistencyException("Unable to remove key:%s in pessimistic mode", key));
                }
            }

            handleStructureChanges(cursorContext);
        } catch (IOException e) {
            exceptionMessageAppender.accept(e);
            throw new UncheckedIOException(e);
        } catch (Throwable e) {
            exceptionMessageAppender.accept(e);
            throw e;
        } finally {
            coordination.reset();
        }

        checkOutOfBounds(cursor);
        return result == InternalTreeLogic.RemoveResult.REMOVED ? removedValue : null;
    }

    private void handleStructureChanges(CursorContext cursorContext) throws IOException {
        if (structurePropagation.hasRightKeyInsert) {
            // New root
            long newRootId = freeList.acquireNewId(stableGeneration, unstableGeneration, cursorContext);
            PageCursorUtil.goTo(cursor, "new root", newRootId);

            bTreeNode.initializeInternal(cursor, treeLogic.layerType, stableGeneration, unstableGeneration);
            bTreeNode.setChildAt(cursor, structurePropagation.midChild, 0, stableGeneration, unstableGeneration);
            bTreeNode.insertKeyAndRightChildAt(
                    cursor,
                    structurePropagation.rightKey,
                    structurePropagation.rightChild,
                    0,
                    0,
                    stableGeneration,
                    unstableGeneration,
                    cursorContext);
            TreeNode.setKeyCount(cursor, 1);
            setRoot(newRootId);
            monitor.treeGrowth();
        } else if (structurePropagation.hasMidChildUpdate) {
            // New successor
            setRoot(structurePropagation.midChild);
        }
        structurePropagation.clear();
    }

    @Override
    public void close() {
        if (!writerLockAcquired) {
            throw appendTreeInformation(
                    new IllegalStateException(format("Tried to close writer, but writer is already closed. %s", this)));
        }
        closeCursor();
        if (parallel) {
            writerLock.readLock().unlock();
        } else {
            writerLock.writeLock().unlock();
        }
        checkpointLock.readLock().unlock();
        writerLockAcquired = false;
    }

    private void closeCursor() {
        if (cursor != null) {
            cursor.close();
            cursor = null;
        }
    }

    @Override
    public String toString() {
        return coordination.toString();
    }
}
