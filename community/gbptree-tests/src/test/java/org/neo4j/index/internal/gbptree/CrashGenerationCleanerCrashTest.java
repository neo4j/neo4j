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

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.neo4j.index.internal.gbptree.CrashGenerationCleaner.MAX_BATCH_SIZE;
import static org.neo4j.index.internal.gbptree.GBPTree.NO_MONITOR;
import static org.neo4j.io.pagecache.context.FixedVersionContextSupplier.EMPTY_CONTEXT_SUPPLIER;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.jupiter.api.Test;
import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.io.pagecache.PagedFile;
import org.neo4j.io.pagecache.StubPagedFile;
import org.neo4j.io.pagecache.context.CursorContext;
import org.neo4j.io.pagecache.context.CursorContextFactory;
import org.neo4j.io.pagecache.tracing.PageCacheTracer;
import org.neo4j.scheduler.Group;
import org.neo4j.scheduler.JobMonitoringParams;
import org.neo4j.test.extension.pagecache.PageCacheExtension;
import org.neo4j.test.scheduler.ThreadPoolJobScheduler;

@PageCacheExtension
class CrashGenerationCleanerCrashTest {
    @Test
    void mustNotLeakTasksOnCrash() {
        // Given
        String exceptionMessage = "When there's no more room in hell, the dead will walk the earth";
        CrashGenerationCleaner cleaner = newCrashingCrashGenerationCleaner(exceptionMessage);
        ExecutorService executorService =
                Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());
        ThreadPoolJobScheduler threadPoolJobScheduler = new ThreadPoolJobScheduler(executorService);

        try {
            var executor = new CleanupJob.Executor() {
                @Override
                public <T> CleanupJob.JobResult<T> submit(String jobDescription, Callable<T> job) {
                    var jobHandle =
                            threadPoolJobScheduler.schedule(Group.TESTING, JobMonitoringParams.NOT_MONITORED, job);
                    return jobHandle::get;
                }
            };
            assertThatThrownBy(() -> cleaner.clean(executor))
                    .rootCause()
                    .isInstanceOf(IOException.class)
                    .hasMessage(exceptionMessage);
        } finally {
            List<Runnable> tasks = executorService.shutdownNow();
            assertEquals(0, tasks.size());
        }
    }

    private static CrashGenerationCleaner newCrashingCrashGenerationCleaner(String message) {
        int pageSize = 8192;
        PagedFile pagedFile = new StubPagedFile(pageSize, 0) {
            final AtomicBoolean first = new AtomicBoolean(true);

            @Override
            public PageCursor io(long pageId, int pf_flags, CursorContext context) throws IOException {
                try {
                    Thread.sleep(1);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
                if (first.getAndSet(false)) {
                    throw new IOException(message);
                }
                return super.io(pageId, pf_flags, context);
            }
        };
        var layout = SimpleLongLayout.longLayout().build();
        return new CrashGenerationCleaner(
                pagedFile,
                null,
                new InternalNodeFixedSize<>(pageSize, layout),
                0,
                MAX_BATCH_SIZE * 1_000_000_000,
                5,
                7,
                NO_MONITOR,
                new CursorContextFactory(PageCacheTracer.NULL, EMPTY_CONTEXT_SUPPLIER),
                "test tree");
    }
}
