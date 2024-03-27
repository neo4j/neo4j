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
package org.neo4j.kernel.impl.index.schema;

import static org.neo4j.index.internal.gbptree.GBPTree.NO_HEADER_READER;

import java.nio.file.OpenOption;
import java.nio.file.Path;
import org.eclipse.collections.api.set.ImmutableSet;
import org.neo4j.annotations.documented.ReporterFactory;
import org.neo4j.common.DependencyResolver;
import org.neo4j.dbms.database.readonly.DatabaseReadOnlyChecker;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.index.internal.gbptree.GBPTree;
import org.neo4j.index.internal.gbptree.MultiRootGBPTree;
import org.neo4j.index.internal.gbptree.RecoveryCleanupWorkCollector;
import org.neo4j.internal.helpers.progress.ProgressMonitorFactory;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.internal.schema.StorageEngineIndexingBehaviour;
import org.neo4j.io.IOUtils;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.context.CursorContextFactory;
import org.neo4j.io.pagecache.tracing.PageCacheTracer;
import org.neo4j.kernel.api.IndexFileSnapshotter;
import org.neo4j.kernel.api.index.IndexProvider;
import org.neo4j.monitoring.Monitors;
import org.neo4j.util.Preconditions;

public class TokenIndex implements ConsistencyCheckable, IndexFileSnapshotter {
    /**
     * Written in header to indicate native token index is clean
     *
     * NOTE that this is not the same byte as the other indexes use,
     * to be able to handle the switch from old scan stores without rebuilding.
     */
    private static final byte CLEAN = (byte) 0x00;

    static final byte ONLINE = CLEAN;

    /**
     * Written in header to indicate native token index is/needs rebuilding
     *
     * NOTE that this is not the same byte as the other indexes use,
     * to be able to handle the switch from old scan stores without rebuilding.
     */
    private static final byte NEEDS_REBUILDING = (byte) 0x01;

    static final byte POPULATING = NEEDS_REBUILDING;

    /**
     * Written in header to indicate native token index failed to population.
     *
     * NOTE that this is not the same byte as the other indexes use,
     * to be able to handle the switch from old scan stores without rebuilding.
     */
    static final byte FAILED = (byte) 0x02;

    /**
     * Checker to verify if this token index is read-only.
     */
    private final DatabaseReadOnlyChecker readOnlyChecker;

    /**
     * Monitors used to pass down monitor to underlying {@link GBPTree}
     */
    private final Monitors monitors;

    /**
     * Tag to use when creating new monitors.
     * We need this because there could be multiple
     * {@link IndexProvider.Monitor listeners} registered
     * of the same type.
     */
    private final String monitorTag;

    /**
     * {@link PageCache} to {@link PageCache#map(Path, int, String, ImmutableSet)}
     * store file backing this token scan store. Passed to {@link GBPTree}.
     */
    private final PageCache pageCache;

    /**
     * IndexFiles wrapping the store file {@link PageCache#map(Path, int, String, ImmutableSet)}.
     */
    final IndexFiles indexFiles;

    /**
     * {@link FileSystemAbstraction} the backing file lives on.
     */
    final FileSystemAbstraction fs;

    private final String databaseName;
    private final CursorContextFactory contextFactory;
    private final ImmutableSet<OpenOption> openOptions;
    private final boolean readOnly;
    private final DependencyResolver dependencyResolver;
    /**
     * The actual index which backs this token index.
     */
    GBPTree<TokenScanKey, TokenScanValue> index;

    /**
     * The single instance of {@link TokenIndexUpdater} used for updates.
     */
    TokenIndexUpdater singleUpdater;

    /**
     * Name of the store that will be used when describing work related to this store.
     */
    private final String tokenStoreName;

    /**
     * Underlying page cache tracer.
     */
    final PageCacheTracer pageCacheTracer;

    /**
     * A descriptor used for monitoring purposes.
     * <p>
     * A descriptor of a token index can change in very rare cases (everything is the same apart from the ID).
     * Since the descriptor is used only for monitoring purposes, we don't have to deal with this edge case here.
     * <p>
     * It is named like this, so no one would be tempted to use it for anything else in the future!
     */
    private final IndexDescriptor monitoringDescriptor;

    protected final TokenIndexIdLayout idLayout;

    public TokenIndex(
            DatabaseIndexContext databaseIndexContext,
            IndexFiles indexFiles,
            IndexDescriptor descriptor,
            ImmutableSet<OpenOption> openOptions,
            boolean readOnly,
            StorageEngineIndexingBehaviour indexingBehaviour) {
        this.readOnlyChecker = databaseIndexContext.readOnlyChecker;
        this.monitors = databaseIndexContext.monitors;
        this.monitorTag = databaseIndexContext.monitorTag;
        this.pageCache = databaseIndexContext.pageCache;
        this.fs = databaseIndexContext.fileSystem;
        this.databaseName = databaseIndexContext.databaseName;
        this.contextFactory = databaseIndexContext.contextFactory;
        this.pageCacheTracer = databaseIndexContext.pageCacheTracer;
        this.dependencyResolver = databaseIndexContext.dependencyResolver;
        this.indexFiles = indexFiles;
        this.tokenStoreName = descriptor.getName();
        this.monitoringDescriptor = descriptor;
        this.openOptions = openOptions;
        this.readOnly = readOnly;
        this.idLayout = TokenIndexIdLayoutFactory.getInstance()
                .createIdLayout(openOptions, descriptor.schema().entityType(), indexingBehaviour);
    }

    void instantiateTree(RecoveryCleanupWorkCollector recoveryCleanupWorkCollector) {
        ensureDirectoryExist();
        MultiRootGBPTree.Monitor monitor = treeMonitor();
        index = new GBPTree<>(
                pageCache,
                fs,
                indexFiles.getStoreFile(),
                new TokenScanLayout(),
                monitor,
                NO_HEADER_READER,
                recoveryCleanupWorkCollector,
                readOnly,
                openOptions,
                databaseName,
                tokenStoreName,
                contextFactory,
                pageCacheTracer,
                dependencyResolver);
    }

    void instantiateUpdater() {
        singleUpdater = new TokenIndexUpdater(1_000, idLayout);
    }

    private MultiRootGBPTree.Monitor treeMonitor() {
        MultiRootGBPTree.Monitor treeMonitor = monitors.newMonitor(MultiRootGBPTree.Monitor.class, monitorTag);
        IndexProvider.Monitor indexMonitor = monitors.newMonitor(IndexProvider.Monitor.class, monitorTag);
        return new IndexMonitorAdaptor(treeMonitor, indexMonitor, indexFiles, monitoringDescriptor);
    }

    private void ensureDirectoryExist() {
        indexFiles.ensureDirectoryExist();
    }

    void closeResources() {
        IOUtils.closeAllUnchecked(index);
        index = null;
    }

    void assertTreeOpen() {
        Preconditions.checkState(index != null, "Index tree has been closed or was never instantiated.");
    }

    @Override
    public boolean consistencyCheck(
            ReporterFactory reporterFactory,
            CursorContextFactory contextFactory,
            int numThreads,
            ProgressMonitorFactory progressMonitorFactory) {
        return index.consistencyCheck(reporterFactory, contextFactory, numThreads, progressMonitorFactory);
    }

    protected void assertWritable() {
        if (readOnlyChecker.isReadOnly()) {
            throw new UnsupportedOperationException(
                    "Database currently is in read only mode and can not perform writes");
        }
    }

    @Override
    public ResourceIterator<Path> snapshotFiles() {
        return indexFiles.snapshot();
    }
}
