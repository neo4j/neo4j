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
package org.neo4j.kernel.impl.api.index.drop;

import static org.neo4j.scheduler.Group.STORAGE_MAINTENANCE;

import java.io.IOException;
import java.nio.file.Path;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.eclipse.collections.impl.factory.Multimaps;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.api.index.IndexProvider;
import org.neo4j.kernel.impl.api.TransactionVisibilityProvider;
import org.neo4j.kernel.impl.api.index.IndexingService;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;
import org.neo4j.scheduler.JobHandle;
import org.neo4j.scheduler.JobScheduler;
import org.neo4j.util.VisibleForTesting;

public final class MultiVersionIndexDropController extends LifecycleAdapter implements IndexDropController {
    private static final int TRIGGER_THRESHOLD = 10;
    private final ConcurrentLinkedDeque<IndexDropRequest> asyncDeleteQueue;
    private final JobScheduler jobScheduler;
    private final IndexingService indexingService;
    private final TransactionVisibilityProvider transactionVisibilityProvider;
    private final Log log;
    private JobHandle<?> asyncDropJobHandle;
    private final FileSystemAbstraction fs;

    public MultiVersionIndexDropController(
            JobScheduler jobScheduler,
            TransactionVisibilityProvider visibilityProvider,
            IndexingService indexingService,
            FileSystemAbstraction fs,
            LogProvider logProvider) {
        this.jobScheduler = jobScheduler;
        this.indexingService = indexingService;
        this.asyncDeleteQueue = new ConcurrentLinkedDeque<>();
        this.transactionVisibilityProvider = visibilityProvider;
        this.fs = fs;
        this.log = logProvider.getLog(MultiVersionIndexDropController.class);
    }

    @Override
    public void start() {
        cleanupLeftOvers();
        asyncDropJobHandle =
                jobScheduler.scheduleRecurring(STORAGE_MAINTENANCE, this::dropIndexes, 1, 1, TimeUnit.MINUTES);
    }

    private void cleanupLeftOvers() {
        try {
            var providers = indexingService.getIndexProviders();
            var providersMap = providers.stream()
                    .collect(Collectors.toMap(
                            indexProvider ->
                                    indexProvider.getProviderDescriptor().name(),
                            indexProvider -> indexProvider));

            var pathMultimap = Multimaps.mutable.set.<IndexProvider, Path>empty();

            for (IndexProvider provider : providers) {
                Path directory = provider.directoryStructure().rootDirectory();
                if (fs.fileExists(directory)) {
                    Path[] indexDirectories = fs.listFiles(directory);
                    for (Path indexDirectory : indexDirectories) {
                        pathMultimap.put(provider, indexDirectory);
                    }
                }
            }
            indexingService.getIndexProxies().forEach(indexProxy -> {
                var provider = providersMap.get(
                        indexProxy.getDescriptor().getIndexProvider().name());
                var path = provider.directoryStructure()
                        .directoryForIndex(indexProxy.getDescriptor().getId());
                pathMultimap.remove(provider, path);
            });

            pathMultimap.forEachValue(path -> {
                try {
                    fs.deleteRecursively(path);
                } catch (IOException e) {
                    log.warn("Failed to remove index directory: " + path, e);
                }
            });
        } catch (Exception e) {
            log.error("Fail to clean up index leftovers.", e);
        }
    }

    @Override
    public void stop() {
        if (asyncDropJobHandle != null) {
            asyncDropJobHandle.cancel();
        }
    }

    @Override
    public void dropIndex(IndexDescriptor descriptor) {
        asyncDeleteQueue.add(
                new IndexDropRequest(descriptor, transactionVisibilityProvider.youngestObservableHorizon()));
        // if we have lots of indexes hanging there schedule cleanup directly
        if (asyncDeleteQueue.size() > TRIGGER_THRESHOLD) {
            jobScheduler.schedule(STORAGE_MAINTENANCE, this::dropIndexes);
        }
    }

    @VisibleForTesting
    ConcurrentLinkedDeque<IndexDropRequest> getAsyncDeleteQueue() {
        return asyncDeleteQueue;
    }

    @VisibleForTesting
    synchronized void dropIndexes() {
        long oldestBoundary = transactionVisibilityProvider.oldestObservableHorizon();

        var request = asyncDeleteQueue.peek();
        while (request != null) {
            if (request.highestOpenedTransaction() < oldestBoundary) {
                safeIndexDrop(request);
                asyncDeleteQueue.remove(request);
                request = asyncDeleteQueue.peek();
            } else {
                request = null;
            }
        }
    }

    private void safeIndexDrop(IndexDropRequest request) {
        try {
            indexingService.internalIndexDrop(request.descriptor);
        } catch (Exception e) {
            log.error("Exception on multi version index async drop.", e);
        }
    }

    private record IndexDropRequest(IndexDescriptor descriptor, long highestOpenedTransaction) {}
}
