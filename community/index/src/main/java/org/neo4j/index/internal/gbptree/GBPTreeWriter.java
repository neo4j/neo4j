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

import static java.lang.String.format;
import static org.neo4j.index.internal.gbptree.Generation.stableGeneration;
import static org.neo4j.index.internal.gbptree.Generation.unstableGeneration;
import static org.neo4j.index.internal.gbptree.PointerChecking.assertNoSuccessor;
import static org.neo4j.index.internal.gbptree.PointerChecking.checkOutOfBounds;
import static org.neo4j.index.internal.gbptree.TreeNodeUtil.generation;
import static org.neo4j.index.internal.gbptree.TreeNodeUtil.isInternal;
import static org.neo4j.index.internal.gbptree.TreeNodeUtil.keyCount;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.function.BooleanSupplier;
import java.util.function.Consumer;
import java.util.function.LongSupplier;
import org.neo4j.index.internal.gbptree.MultiRootGBPTree.Monitor;
import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.io.pagecache.PageCursorUtil;
import org.neo4j.io.pagecache.PagedFile;
import org.neo4j.io.pagecache.context.CursorContext;

class GBPTreeWriter<K, V> implements Writer<K, V> {
    private final InternalTreeLogic<K, V> treeLogic;
    private final ReadWriteLock checkpointLock;
    private final ReadWriteLock writerLock;
    private final FreeListIdProvider freeList;
    private final Monitor monitor;
    private final Consumer<Throwable> exceptionMessageAppender;
    private final LongSupplier generationSupplier;
    private final BooleanSupplier mustEagerlyFlushSupplier;
    private final StructureWriteLog.Session structureWriteLog;
    private final StructurePropagation<K> structurePropagation;
    private final PagedFile pagedFile;
    private final TreeWriterCoordination coordination;
    private final LeafNodeBehaviour<K, V> leafNode;
    private final InternalNodeBehaviour<K> internalNode;
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
            LeafNodeBehaviour<K, V> leafNode,
            InternalNodeBehaviour<K> internalNode,
            boolean parallel,
            TreeRootExchange rootExchange,
            ReadWriteLock checkpointLock,
            ReadWriteLock writerLock,
            FreeListIdProvider freeList,
            Monitor monitor,
            Consumer<Throwable> exceptionMessageAppender,
            LongSupplier generationSupplier,
            BooleanSupplier mustEagerlyFlushSupplier,
            StructureWriteLog.Session structureWriteLog) {
        this.layout = layout;
        this.pagedFile = pagedFile;
        this.coordination = coordination;
        this.leafNode = leafNode;
        this.internalNode = internalNode;
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
        this.mustEagerlyFlushSupplier = mustEagerlyFlushSupplier;
        this.structureWriteLog = structureWriteLog;
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
            cursor = pagedFile.io(0L /*Ignored*/, writeCursorFlags(), cursorContext);
            coordination.initialize(cursor);
            this.cursorContext = cursorContext;
            long generation = generationSupplier.getAsLong();
            stableGeneration = stableGeneration(generation);
            unstableGeneration = unstableGeneration(generation);
            this.ratioToKeepInLeftOnSplit = ratioToKeepInLeftOnSplit;
            root = rootExchange.getRoot(cursorContext);
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

    private int writeCursorFlags() {
        var flags = PagedFile.PF_SHARED_WRITE_LOCK;
        if (mustEagerlyFlushSupplier.getAsBoolean()) {
            flags |= PagedFile.PF_EAGER_FLUSH;
        }
        return flags;
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
            coordination.beginOperation();
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
                valueMerger.reset();
                assert structurePropagation.isEmpty();
                treeLogic.reset();
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
            checkForceReset();
        }

        checkOutOfBounds(cursor);
    }

    /**
     * @return true if operation is permitted
     */
    private boolean goToRoot() throws IOException {
        if (treeLogic.depth() >= 0) {
            return true;
        }

        while (true) {
            coordination.beforeTraversingToChild(root.id(), 0);
            // check again, after locked
            Root rootAfterLock = rootExchange.getRoot(cursorContext);
            if (!rootAfterLock.equals(root)) {
                // There was a root change in between getting the root id and locking it
                coordination.reset();
                root = rootAfterLock;
            } else {
                TreeNodeUtil.goTo(cursor, "Root", root.id());
                break;
            }
        }

        assert assertNoSuccessor(cursor, stableGeneration, unstableGeneration);
        treeLogic.initialize(cursor, ratioToKeepInLeftOnSplit, structureWriteLog);
        int keyCount = keyCount(cursor);
        var isInternal = isInternal(cursor);
        return coordination.arrivedAtChild(
                isInternal,
                (isInternal ? internalNode : leafNode).availableSpace(cursor, keyCount),
                generation(cursor) != unstableGeneration,
                keyCount);
    }

    private void setRoot(long rootPointer) throws IOException {
        long rootId = GenerationSafePointerPair.pointer(rootPointer);
        rootExchange.setRoot(new Root(rootId, unstableGeneration), cursorContext);
    }

    @Override
    public V remove(K key) {
        var removedValue = new ValueHolder<>(layout.newValue());
        InternalTreeLogic.RemoveResult result;
        try {
            // Try optimistic mode
            coordination.beginOperation();
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
                treeLogic.reset();
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
            checkForceReset();
        }

        checkOutOfBounds(cursor);
        return result == InternalTreeLogic.RemoveResult.REMOVED && removedValue.defined ? removedValue.value : null;
    }

    @Override
    public void execute(TreeWriteOperation<K, V> operation) {
        try {
            executeWithRetryInPessimisticMode(operation);
            handleStructureChanges(cursorContext);
        } catch (IOException e) {
            exceptionMessageAppender.accept(e);
            throw new UncheckedIOException(e);
        } catch (Throwable t) {
            exceptionMessageAppender.accept(t);
            throw t;
        } finally {
            checkForceReset();
        }
        checkOutOfBounds(cursor);
    }

    private void setRootUnchecked(long root) {
        try {
            setRoot(root);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private void executeWithRetryInPessimisticMode(TreeWriteOperation<K, V> operation) throws IOException {
        coordination.beginOperation();
        if (goToRoot()
                && operation.run(
                        layout,
                        treeLogic,
                        cursor,
                        structurePropagation,
                        stableGeneration,
                        unstableGeneration,
                        cursorContext,
                        this::setRootUnchecked,
                        freeList)) {
            return;
        }

        // operation wasn't permitted by goToRoot or failed, retry in pessimistic mode
        coordination.flipToPessimisticMode();
        assert structurePropagation.isEmpty();
        treeLogic.reset();
        if (goToRoot()
                && operation.run(
                        layout,
                        treeLogic,
                        cursor,
                        structurePropagation,
                        stableGeneration,
                        unstableGeneration,
                        cursorContext,
                        this::setRootUnchecked,
                        freeList)) {
            return;
        }
        throw appendTreeInformation(
                new TreeInconsistencyException("Unable to perform operation " + operation + " in pessimistic mode"));
    }

    private void checkForceReset() {
        if (coordination.checkForceReset()) {
            // After pessimistic (and at some frequency for parallel writer) the tree is pretty much locked tight
            // so force a reset for the next operation
            reset();
        }
    }

    private void reset() {
        treeLogic.reset();
        coordination.reset();
    }

    @Override
    public void yield() {
        reset();
    }

    private void handleStructureChanges(CursorContext cursorContext) throws IOException {
        var didRootStructureChange = false;
        if (structurePropagation.hasRightKeyInsert) {
            // New root
            long newRootId = freeList.acquireNewId(stableGeneration, unstableGeneration, CursorCreator.bind(cursor));
            PageCursorUtil.goTo(cursor, "new root", newRootId);

            structureWriteLog.growTree(unstableGeneration, newRootId);

            internalNode.initialize(cursor, treeLogic.layerType, stableGeneration, unstableGeneration);
            internalNode.setChildAt(cursor, structurePropagation.midChild, 0, stableGeneration, unstableGeneration);
            internalNode.insertKeyAndRightChildAt(
                    cursor,
                    structurePropagation.rightKey,
                    structurePropagation.rightChild,
                    0,
                    0,
                    stableGeneration,
                    unstableGeneration,
                    cursorContext);
            TreeNodeUtil.setKeyCount(cursor, 1);
            setRoot(newRootId);
            monitor.treeGrowth();
            didRootStructureChange = true;
        } else if (structurePropagation.hasMidChildUpdate) {
            // New successor
            setRoot(structurePropagation.midChild);
            didRootStructureChange = true;
        }
        structurePropagation.clear();

        if (didRootStructureChange) {
            // This is for mitigating the temporary mismatch in depth state between coordination and treeLogic
            // where the coordination must retain the latch on the root until the root structure change
            // has been completed.
            coordination.reset();
            treeLogic.reset();
        }
    }

    @Override
    public void close() {
        if (!writerLockAcquired) {
            throw appendTreeInformation(
                    new IllegalStateException(format("Tried to close writer, but writer is already closed. %s", this)));
        }
        closeCursor();
        coordination.close();
        treeLogic.reset();
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
