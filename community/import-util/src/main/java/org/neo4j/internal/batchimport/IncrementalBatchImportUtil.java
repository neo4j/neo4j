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
package org.neo4j.internal.batchimport;

import static java.lang.String.format;
import static org.neo4j.dbms.database.readonly.DatabaseReadOnlyChecker.readOnly;
import static org.neo4j.internal.helpers.collection.Iterators.filter;
import static org.neo4j.internal.helpers.collection.Iterators.firstOrNull;

import java.io.Closeable;
import java.io.IOException;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.OptionalLong;
import java.util.stream.Collectors;
import org.eclipse.collections.api.block.function.primitive.LongToLongFunction;
import org.eclipse.collections.api.set.ImmutableSet;
import org.eclipse.collections.api.set.primitive.LongSet;
import org.eclipse.collections.impl.factory.primitive.LongSets;
import org.neo4j.batchimport.api.Configuration;
import org.neo4j.batchimport.api.input.Input;
import org.neo4j.common.TokenNameLookup;
import org.neo4j.configuration.Config;
import org.neo4j.internal.batchimport.cache.idmapping.IndexIdMapper;
import org.neo4j.internal.helpers.progress.ProgressListener;
import org.neo4j.internal.kernel.api.InternalIndexState;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.internal.schema.SchemaCache;
import org.neo4j.internal.schema.SchemaDescriptor;
import org.neo4j.internal.schema.SchemaUserDescription;
import org.neo4j.internal.schema.StorageEngineIndexingBehaviour;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.layout.DatabaseFile;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.io.layout.PlainDatabaseLayout;
import org.neo4j.io.locker.Locker;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.context.CursorContext;
import org.neo4j.io.pagecache.context.CursorContextFactory;
import org.neo4j.io.pagecache.tracing.FileFlushEvent;
import org.neo4j.io.pagecache.tracing.PageCacheTracer;
import org.neo4j.kernel.api.exceptions.index.IndexEntryConflictException;
import org.neo4j.kernel.api.index.IndexAccessor;
import org.neo4j.kernel.api.index.IndexEntryConflictHandler;
import org.neo4j.kernel.api.index.IndexProvider;
import org.neo4j.kernel.api.index.IndexProvidersAccess;
import org.neo4j.kernel.impl.api.index.IndexProviderMap;
import org.neo4j.kernel.impl.api.index.IndexSamplingConfig;
import org.neo4j.kernel.impl.api.index.stats.IndexStatisticsStore;
import org.neo4j.scheduler.JobScheduler;
import org.neo4j.token.TokenHolders;
import org.neo4j.util.Preconditions;

/**
 * Let's start by just gathering stuff that is common between the (now) two implementations.
 */
public class IncrementalBatchImportUtil {
    public static Closeable acquireTargetDatabaseLock(FileSystemAbstraction fileSystem, DatabaseLayout databaseLayout)
            throws IOException {
        var locker = new Locker(fileSystem, databaseLayout.databaseLockFile());
        var success = false;
        try {
            locker.checkLock();
            success = true;
        } finally {
            if (!success) {
                locker.close();
            }
        }
        return locker;
    }

    public static void copyStoreFiles(
            FileSystemAbstraction fileSystem,
            PlainDatabaseLayout fromLayout,
            DatabaseLayout toLayout,
            DatabaseFile... databaseFiles)
            throws IOException {
        var paths = new ArrayList<Path>();
        for (var databaseFile : databaseFiles) {
            paths.add(fromLayout.file(databaseFile));
            fromLayout.idFile(databaseFile).ifPresent(paths::add);
        }
        copyStoreFiles(fileSystem, toLayout, paths.toArray(new Path[0]));
    }

    public static void copyStoreFiles(FileSystemAbstraction fileSystem, DatabaseLayout into, Path... paths)
            throws IOException {
        for (Path path : paths) {
            fileSystem.copyFile(path, into.file(path.getFileName().toString()));
        }
    }

    /**
     * Copies indexes matching {@code schemaDescriptors} (which represents the ID-mapper indexes) as well as
     * indexes backing uniqueness constraints.
     *
     * @param schemaDescriptors {@link Map} from input group name to {@link SchemaDescriptor}.
     * @return a {@link Map} from input group name to actual index ID.
     * @throws IOException on I/O error copying files.
     */
    public static Map<String, Long> copyIndexFilesFromTargetDatabase(
            FileSystemAbstraction fileSystem,
            IndexProvidersAccess indexProvidersAccess,
            DatabaseLayout databaseLayout,
            PageCache pageCache,
            DatabaseLayout incrementalDatabaseLayout,
            Map<String, SchemaDescriptor> schemaDescriptors,
            SchemaCache schemaCache,
            TokenNameLookup tokenNameLookup,
            ImmutableSet<OpenOption> openOptions,
            CursorContextFactory contextFactory)
            throws IOException {
        // Figure out which indexes will be used for the ID mapping in the build phase
        var targetIndexProviders = indexProvidersAccess.access(pageCache, databaseLayout, readOnly());
        var incrementalIndexProviders = indexProvidersAccess.access(pageCache, incrementalDatabaseLayout, readOnly());
        var idMapperIndexes = findIdMapperIndexes(
                schemaDescriptors, schemaCache, tokenNameLookup, openOptions, contextFactory, targetIndexProviders);
        var copiedIndexIds = LongSets.mutable.empty();
        for (var entry : idMapperIndexes.entrySet()) {
            var descriptor = entry.getValue();
            assertOwningConstraintExists(schemaCache, descriptor, tokenNameLookup);
            if (copiedIndexIds.add(descriptor.getId())) {
                copyIndex(fileSystem, targetIndexProviders, incrementalIndexProviders, descriptor);
                copiedIndexIds.add(descriptor.getId());
            }
        }

        // Copy all indexes that are backing uniqueness constraints
        // TODO try to limit the amount of indexes that need to be copied, based on information from the input?
        for (var constraint : schemaCache.constraints()) {
            if (constraint.enforcesUniqueness()) {
                var indexId = constraint.asIndexBackedConstraint().ownedIndexId();
                if (copiedIndexIds.add(indexId)) {
                    copyIndex(
                            fileSystem, targetIndexProviders, incrementalIndexProviders, schemaCache.getIndex(indexId));
                }
            }
        }
        return idMapperIndexes.entrySet().stream()
                .collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue().getId()));
    }

    public static Map<String, IndexDescriptor> findIdMapperIndexes(
            Map<String, SchemaDescriptor> schemaDescriptors,
            SchemaCache schemaCache,
            TokenNameLookup tokenNameLookup,
            ImmutableSet<OpenOption> openOptions,
            CursorContextFactory contextFactory,
            IndexProviderMap targetIndexProviders) {
        var idMapperIndexes = new HashMap<String, IndexDescriptor>();
        for (var entry : schemaDescriptors.entrySet()) {
            var descriptor = findLikelyIndex(schemaCache, entry.getValue(), tokenNameLookup);
            if (idMapperIndexes.put(entry.getKey(), descriptor) == null) {
                var targetIndexProvider = targetIndexProviders.lookup(descriptor.getIndexProvider());
                assertIndexIsOnline(targetIndexProvider, descriptor, openOptions, tokenNameLookup, contextFactory);
            }
        }
        return idMapperIndexes;
    }

    private static IndexDescriptor findLikelyIndex(
            SchemaCache schemaCache, SchemaDescriptor schemaDescriptor, TokenNameLookup tokenNameLookup) {
        IndexDescriptor descriptor =
                firstOrNull(filter(IndexDescriptor::isUnique, schemaCache.indexesForSchema(schemaDescriptor)));
        Preconditions.checkState(
                descriptor != null,
                "Couldn't find a matching index for %s",
                schemaDescriptor.userDescription(tokenNameLookup));
        return descriptor;
    }

    private static void assertOwningConstraintExists(
            SchemaCache schemaCache, IndexDescriptor descriptor, TokenNameLookup tokenNameLookup) {
        OptionalLong owningConstraintId = descriptor.getOwningConstraintId();
        if (owningConstraintId.isPresent()) {
            long constraintId = owningConstraintId.getAsLong();
            Preconditions.checkState(
                    schemaCache.hasConstraintRule(constraintId),
                    "Couldn't find a uniqueness constraint for %s",
                    descriptor.userDescription(tokenNameLookup));
        }
    }

    private static void assertIndexIsOnline(
            IndexProvider indexProvider,
            IndexDescriptor descriptor,
            ImmutableSet<OpenOption> openOptions,
            TokenNameLookup tokenNameLookup,
            CursorContextFactory contextFactory) {
        try (var cursorContext = contextFactory.create("Check index online")) {
            var state = indexProvider.getInitialState(descriptor, cursorContext, openOptions);
            Preconditions.checkState(
                    state == InternalIndexState.ONLINE,
                    "Index %s to use for ID mapping is not online, but %s",
                    descriptor.userDescription(tokenNameLookup),
                    state);
        }
    }

    public static void copyIndex(
            FileSystemAbstraction fileSystem,
            IndexProviderMap fromIndexProviders,
            IndexProviderMap toIndexProviders,
            IndexDescriptor indexDescriptor)
            throws IOException {
        var from = fromIndexProviders
                .lookup(indexDescriptor.getIndexProvider())
                .directoryStructure()
                .directoryForIndex(indexDescriptor.getId());
        var to = toIndexProviders
                .lookup(indexDescriptor.getIndexProvider())
                .directoryStructure()
                .directoryForIndex(indexDescriptor.getId());
        fileSystem.deleteRecursively(to);
        fileSystem.mkdirs(to.getParent());
        fileSystem.copyRecursively(from, to);
    }

    public static void moveIndex(
            FileSystemAbstraction fileSystem,
            IndexProviderMap fromIndexProviders,
            IndexProviderMap toIndexProviders,
            IndexDescriptor indexDescriptor)
            throws IOException {
        var from = fromIndexProviders
                .lookup(indexDescriptor.getIndexProvider())
                .directoryStructure()
                .directoryForIndex(indexDescriptor.getId());
        var to = toIndexProviders
                .lookup(indexDescriptor.getIndexProvider())
                .directoryStructure()
                .directoryForIndex(indexDescriptor.getId());
        fileSystem.deleteRecursively(to);
        fileSystem.mkdirs(to.getParent());
        fileSystem.renameFile(from, to, StandardCopyOption.ATOMIC_MOVE);
    }

    public static DatabaseLayout findPreparedIncrementalDatabaseLayout(DatabaseLayout databaseLayout) {
        var name = databaseLayout.getNeo4jLayout().databaseLayouts().stream()
                .map(DatabaseLayout::getDatabaseName)
                .filter(databaseName ->
                        databaseName.matches(format("^%s-incremental-\\d+$", databaseLayout.getDatabaseName())))
                .max(Comparator.comparingLong(IncrementalBatchImportUtil::timeStampOf))
                .orElseThrow(() -> new RuntimeException(
                        "No prepared incremental import location to " + databaseLayout.getDatabaseName() + " found"));
        return DatabaseLayout.of(databaseLayout.getNeo4jLayout(), name);
    }

    private static long timeStampOf(String incrementalDatabaseName) {
        int startIndex = -1;
        int stringLength = incrementalDatabaseName.length();
        for (int i = stringLength - 1; i >= 0; i--) {
            if (Character.isDigit(incrementalDatabaseName.charAt(i))) {
                startIndex = i;
            } else {
                break;
            }
        }
        Preconditions.checkState(startIndex != -1, "Invalid incremental database folder " + incrementalDatabaseName);
        return Long.parseLong(incrementalDatabaseName.substring(startIndex));
    }

    public static IndexIdMapper buildIndexIdMapper(
            Input input,
            Configuration config,
            PageCacheTracer pageCacheTracer,
            PopulationWorkJobScheduler workScheduler,
            IndexProviderMap indexProviders,
            SchemaCache schemaCache,
            Map<String, Long> idMapperIndexes,
            TokenHolders tokenHolders,
            ImmutableSet<OpenOption> openOptions,
            IndexProviderMap tempNewNodesIndexProviders,
            IndexStatisticsStore indexStatisticsStore,
            StorageEngineIndexingBehaviour indexingBehaviour)
            throws IOException {
        Map<String, IndexAccessor> accessors = new HashMap<>();
        Map<String, IndexDescriptor> indexDescriptors = new HashMap<>();
        var indexSamplingConfig = new IndexSamplingConfig(Config.defaults());
        for (var entry : idMapperIndexes.entrySet()) {
            var indexDescriptor = schemaCache.getIndex(entry.getValue());
            // Remove this from the schema cache so that it won't be part of detecting other affected index changes
            schemaCache.removeSchemaRule(entry.getValue());
            var accessor = indexProviders
                    .lookup(indexDescriptor.getIndexProvider())
                    .getOnlineAccessor(
                            indexDescriptor,
                            indexSamplingConfig,
                            SchemaUserDescription.TOKEN_ID_NAME_LOOKUP,
                            openOptions,
                            indexingBehaviour);
            accessors.put(entry.getKey(), accessor);
            indexDescriptors.put(entry.getKey(), indexDescriptor);
        }

        var inputSchema = input.referencedNodeSchema(tokenHolders);
        Preconditions.checkState(
                inputSchema.equals(asSchemaDescriptors(indexDescriptors)),
                "Referenced node schema from 'prepare':%s differs from that in 'build':%s.",
                indexDescriptors,
                inputSchema);

        return new IndexIdMapper(
                accessors,
                tempNewNodesIndexProviders,
                tokenHolders,
                indexDescriptors,
                workScheduler,
                openOptions,
                config,
                pageCacheTracer,
                indexStatisticsStore,
                input.groups(),
                indexingBehaviour);
    }

    public static void mergeIndexes(
            LongSet affectedIndexes,
            SchemaCache targetSchemaCache,
            TokenHolders targetTokenHolders,
            IndexProviderMap targetIndexProviders,
            TokenHolders incrementalTokenHolders,
            IndexProviderMap incrementalIndexProviders,
            ImmutableSet<OpenOption> openOptions,
            LongToLongFunction entityIdConverter,
            ProgressListener progress,
            List<Closeable> toCloseBeforePageCacheClose,
            StorageEngineIndexingBehaviour indexingBehaviour,
            FileSystemAbstraction fileSystem,
            Config dbConfig,
            CursorContextFactory contextFactory,
            Configuration config,
            JobScheduler jobScheduler)
            throws IOException {
        // This method assumes that no writes were made on target while building the increment

        // Just copy the affected indexes from the increment to target
        var indexIds = affectedIndexes.longIterator();
        while (indexIds.hasNext()) {
            var indexId = indexIds.next();
            var index = targetSchemaCache.getIndex(indexId);

            // If this is a constraint index then copy it from what we built
            if (index.isUnique()) {
                progress.add(estimateIndexSize(
                        targetSchemaCache.getIndex(indexId),
                        incrementalIndexProviders,
                        incrementalTokenHolders,
                        openOptions,
                        indexingBehaviour,
                        dbConfig,
                        contextFactory));
                moveIndex(fileSystem, incrementalIndexProviders, targetIndexProviders, index);
            } else {
                try (var incrementalIndex = incrementalIndexProviders
                        .lookup(index.getIndexProvider())
                        .getOnlineAccessor(
                                index,
                                new IndexSamplingConfig(Config.defaults()),
                                incrementalTokenHolders,
                                openOptions,
                                indexingBehaviour)) {
                    var targetIndex = targetIndexProviders
                            .lookup(index.getIndexProvider())
                            .getOnlineAccessor(
                                    index,
                                    new IndexSamplingConfig(Config.defaults()),
                                    targetTokenHolders,
                                    openOptions,
                                    indexingBehaviour);
                    try {
                        targetIndex.insertFrom(
                                incrementalIndex,
                                entityIdConverter,
                                false,
                                IndexEntryConflictHandler.THROW,
                                null,
                                config.maxNumberOfWorkerThreads(),
                                jobScheduler,
                                progress);
                        toCloseBeforePageCacheClose.add(() -> {
                            try (targetIndex) {
                                targetIndex.force(FileFlushEvent.NULL, CursorContext.NULL_CONTEXT);
                            }
                        });
                    } catch (IndexEntryConflictException e) {
                        throw new RuntimeException(e);
                    }
                }
            }
            progress.mark('-');
        }
    }

    public static long estimateIndexSize(
            IndexDescriptor indexDescriptor,
            IndexProviderMap indexProviders,
            TokenHolders tokenHolders,
            ImmutableSet<OpenOption> openOptions,
            StorageEngineIndexingBehaviour indexingBehaviour,
            Config dbConfig,
            CursorContextFactory contextFactory)
            throws IOException {
        try (var incrementalIndex = indexProviders
                        .lookup(indexDescriptor.getIndexProvider())
                        .getOnlineAccessor(
                                indexDescriptor,
                                new IndexSamplingConfig(dbConfig),
                                tokenHolders,
                                openOptions,
                                indexingBehaviour);
                var context = contextFactory.create("estimate index size")) {
            return incrementalIndex.estimateNumberOfEntries(context);
        }
    }

    private static Map<String, SchemaDescriptor> asSchemaDescriptors(Map<String, IndexDescriptor> indexDescriptors) {
        return indexDescriptors.entrySet().stream()
                .collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue().schema()));
    }

    public enum ImportState {
        PREPARE_STARTED("prepare.started"),
        PREPARE_COMPLETED("prepare.completed"),
        BUILD_STARTED("build.started"),
        BUILD_COMPLETED("build.completed"),
        MERGE_STARTED("merge.started"),
        MERGE_COMPLETED("merge.completed");

        public static final ImportState[] VALUES = values();

        public final String info;

        ImportState(String info) {
            this.info = info;
        }
    }
}
