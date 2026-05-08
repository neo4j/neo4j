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
package org.neo4j.internal.id;

import static org.assertj.core.api.Assertions.assertThat;
import static org.eclipse.collections.impl.factory.Sets.immutable;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;
import static org.neo4j.dbms.database.readonly.DatabaseReadOnlyChecker.writable;
import static org.neo4j.index.internal.gbptree.RecoveryCleanupWorkCollector.immediate;
import static org.neo4j.internal.id.IdSlotDistribution.SINGLE_IDS;
import static org.neo4j.io.pagecache.context.CursorContext.NULL_CONTEXT;
import static org.neo4j.io.pagecache.context.FixedVersionContextSupplier.EMPTY_CONTEXT_SUPPLIER;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.neo4j.adversaries.CountingAdversary;
import org.neo4j.adversaries.MethodGuardedAdversary;
import org.neo4j.adversaries.fs.AdversarialFileSystemAbstraction;
import org.neo4j.configuration.Config;
import org.neo4j.configuration.DatabaseConfig;
import org.neo4j.internal.id.indexed.IndexedIdGenerator;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.fs.ReadAheadChannel;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.context.CursorContextFactory;
import org.neo4j.io.pagecache.tracing.DefaultPageCacheTracer;
import org.neo4j.io.pagecache.tracing.PageCacheTracer;
import org.neo4j.kernel.lifecycle.LifeSupport;
import org.neo4j.logging.internal.NullLogService;
import org.neo4j.memory.EmptyMemoryTracker;
import org.neo4j.test.Barrier;
import org.neo4j.test.Race;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.LifeExtension;
import org.neo4j.test.extension.pagecache.EphemeralPageCacheExtension;
import org.neo4j.test.scheduler.ThreadPoolJobScheduler;
import org.neo4j.test.utils.TestDirectory;

@EphemeralPageCacheExtension
@ExtendWith(LifeExtension.class)
class BufferedIdControllerTest {
    @Inject
    private TestDirectory testDirectory;

    @Inject
    private FileSystemAbstraction fs;

    @Inject
    private PageCache pageCache;

    @Inject
    private LifeSupport life;

    private BufferingIdGeneratorFactory idGeneratorFactory;
    private BufferedIdController controller;

    void setUp(
            CursorContextFactory contextFactory,
            FileSystemAbstraction filesystem,
            IndexedIdGenerator.Monitor idGeneratorMonitor)
            throws IOException {
        idGeneratorFactory = new BufferingIdGeneratorFactory(new DefaultIdGeneratorFactory(
                filesystem,
                immediate(),
                true,
                PageCacheTracer.NULL,
                DEFAULT_DATABASE_NAME,
                true,
                true,
                idGeneratorMonitor));
        Config globalConfig = Config.defaults();
        ThreadPoolJobScheduler scheduler = new ThreadPoolJobScheduler();
        controller = new BufferedIdController(
                idGeneratorFactory,
                scheduler,
                contextFactory,
                new DatabaseConfig(globalConfig),
                "test db",
                NullLogService.getInstance());
        controller.initialize(
                filesystem,
                testDirectory.file("buffer"),
                globalConfig,
                () -> new IdController.TransactionSnapshot(10, 0, 0),
                new IdController.VisibilityHorizonVisibilityBoundary() {
                    @Override
                    public long oldestCleanupHorizon() {
                        return 9;
                    }

                    @Override
                    public long oldestVisibilityHorizon() {
                        return 9;
                    }
                },
                s -> true,
                EmptyMemoryTracker.INSTANCE,
                writable());
        life.add(controller);
        life.add(scheduler);
    }

    @Test
    void shouldStopWhenNotStarted() throws IOException {
        setUp(new CursorContextFactory(PageCacheTracer.NULL, EMPTY_CONTEXT_SUPPLIER), fs, null);

        assertDoesNotThrow(controller::stop);
    }

    @Test
    void reportPageCacheMetricsOnMaintenance() throws IOException {
        var pageCacheTracer = new DefaultPageCacheTracer();
        var contextFactory = new CursorContextFactory(pageCacheTracer, EMPTY_CONTEXT_SUPPLIER);
        setUp(contextFactory, fs, null);

        try (var idGenerator = idGeneratorFactory.create(
                pageCache,
                testDirectory.file("foo"),
                TestIdType.TEST,
                100L,
                true,
                1000L,
                false,
                Config.defaults(),
                contextFactory,
                immutable.empty(),
                SINGLE_IDS)) {
            idGenerator.start(FreeIds.NO_FREE_IDS, NULL_CONTEXT);
            try (var marker = idGenerator.transactionalMarker(NULL_CONTEXT)) {
                marker.markDeleted(1L);
            }
            idGenerator.clearCache(true, NULL_CONTEXT);

            long initialPins = pageCacheTracer.pins();
            long initialUnpins = pageCacheTracer.unpins();
            long initialHits = pageCacheTracer.hits();

            controller.maintenance();

            assertThat(pageCacheTracer.pins() - initialPins).isEqualTo(1);
            assertThat(pageCacheTracer.unpins() - initialUnpins).isEqualTo(1);
            assertThat(pageCacheTracer.hits() - initialHits).isEqualTo(1);
        }
    }

    @RepeatedTest(10)
    void concurrentMaintenanceAndClose() throws Throwable {
        var race = new Race();
        var pageCacheTracer = new DefaultPageCacheTracer();
        var contextFactory = new CursorContextFactory(pageCacheTracer, EMPTY_CONTEXT_SUPPLIER);
        setUp(contextFactory, fs, null);
        life.start();
        try (var idGenerator = idGeneratorFactory.create(
                pageCache,
                testDirectory.file("foo"),
                TestIdType.TEST,
                100L,
                true,
                1000L,
                false,
                Config.defaults(),
                contextFactory,
                immutable.empty(),
                SINGLE_IDS)) {

            idGenerator.start(FreeIds.NO_FREE_IDS, NULL_CONTEXT);
            try (var marker = idGenerator.transactionalMarker(NULL_CONTEXT)) {
                for (int i = 0; i < 1000; i++) {
                    marker.markUsed(i);
                    marker.markDeleted(i);
                }
            }

            race.addContestant(controller::maintenance);
            race.addContestant(() -> {
                try {
                    life.shutdown();
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            });
            race.go();
        }
    }

    @Test
    void maintenanceAfterClose() throws Throwable {
        var pageCacheTracer = new DefaultPageCacheTracer();
        var contextFactory = new CursorContextFactory(pageCacheTracer, EMPTY_CONTEXT_SUPPLIER);
        setUp(contextFactory, fs, null);
        life.start();
        try (var idGenerator = idGeneratorFactory.create(
                pageCache,
                testDirectory.file("foo"),
                TestIdType.TEST,
                100L,
                true,
                1000L,
                false,
                Config.defaults(),
                contextFactory,
                immutable.empty(),
                SINGLE_IDS)) {

            idGenerator.start(FreeIds.NO_FREE_IDS, NULL_CONTEXT);
            try (var marker = idGenerator.transactionalMarker(NULL_CONTEXT)) {
                for (int i = 0; i < 1000; i++) {
                    marker.markUsed(i);
                    marker.markDeleted(i);
                }
            }

            life.shutdown();
            controller.maintenance();
        }
    }

    @Test
    void maintenanceWithAdversary() throws Throwable {
        var adversary = new MethodGuardedAdversary(
                new CountingAdversary(1, false),
                DiskBufferedIds.class.getDeclaredMethod(
                        "processChunk", BufferedIds.BufferedIdVisitor.class, ReadAheadChannel.class));
        var pageCacheTracer = new DefaultPageCacheTracer();
        var contextFactory = new CursorContextFactory(pageCacheTracer, EMPTY_CONTEXT_SUPPLIER);
        var adversarialFs = new AdversarialFileSystemAbstraction(adversary, fs);
        setUp(contextFactory, adversarialFs, null);
        life.start();
        try (var idGenerator = idGeneratorFactory.create(
                pageCache,
                testDirectory.file("foo"),
                TestIdType.TEST,
                100L,
                true,
                1000L,
                false,
                Config.defaults(),
                contextFactory,
                immutable.empty(),
                SINGLE_IDS)) {

            idGenerator.start(FreeIds.NO_FREE_IDS, NULL_CONTEXT);
            try (var marker = idGenerator.transactionalMarker(NULL_CONTEXT)) {
                for (int i = 0; i < 1000; i++) {
                    marker.markUsed(i);
                    marker.markDeleted(i);
                }
            }

            controller.maintenance();
        }
    }

    @Test
    void shouldAllowFreeIdsJobToRunUnaffectedByLoadIdsJob() throws Exception {
        // given
        var monitorSlownessEnabled = new AtomicBoolean();
        var monitorSlownessBarrier = new Barrier.Control();
        var monitor = new IndexedIdGenerator.Monitor() {
            @Override
            public void markedAsReserved(long markedId, int numberOfIds) {
                if (monitorSlownessEnabled.get()) {
                    monitorSlownessBarrier.reached();
                    monitorSlownessEnabled.set(false);
                }
            }
        };
        setUp(CursorContextFactory.NULL_CONTEXT_FACTORY, fs, monitor);
        try (var idGenerator = idGeneratorFactory.create(
                pageCache,
                testDirectory.file("foo"),
                TestIdType.TEST,
                0,
                false,
                Long.MAX_VALUE,
                false,
                Config.defaults(),
                CursorContextFactory.NULL_CONTEXT_FACTORY,
                immutable.empty(),
                SINGLE_IDS)) {
            idGenerator.start(
                    (FreeIds) visitor -> {
                        int numFreeIds = 100;
                        for (int i = 0; i < numFreeIds; i++) {
                            visitor.accept(i);
                        }
                        return numFreeIds - 1;
                    },
                    NULL_CONTEXT);
            idGenerator.clearCache(true, NULL_CONTEXT);

            monitorSlownessEnabled.set(true);
            life.start();

            // when
            monitorSlownessBarrier.await();

            // then
            for (int i = 0; i < 3; i++) {
                controller.maintenance(IdController.MAINTENANCE_FREE_IDS);
            }
            monitorSlownessBarrier.release();
        }
    }
}
