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
package org.neo4j.kernel.database;

import static java.lang.String.format;
import static org.assertj.core.api.AssertionsForInterfaceTypes.assertThat;
import static org.eclipse.collections.impl.factory.Sets.mutable;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.collection.Dependencies.dependenciesOf;
import static org.neo4j.logging.AssertableLogProvider.Level.INFO;
import static org.neo4j.logging.AssertableLogProvider.Level.WARN;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteOrder;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import org.eclipse.collections.api.set.ImmutableSet;
import org.eclipse.collections.api.set.MutableSet;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.neo4j.common.DependencyResolver;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.index.internal.gbptree.GBPTreeStructure;
import org.neo4j.index.internal.gbptree.GBPTreeVisitor;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.io.pagecache.DelegatingPageCache;
import org.neo4j.io.pagecache.DelegatingPagedFile;
import org.neo4j.io.pagecache.IOController;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.PageCacheOpenOptions;
import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.io.pagecache.PagedFile;
import org.neo4j.io.pagecache.context.CursorContext;
import org.neo4j.io.pagecache.impl.muninn.EvictionBouncer;
import org.neo4j.io.pagecache.impl.muninn.VersionStorage;
import org.neo4j.io.pagecache.tracing.DatabaseFlushEvent;
import org.neo4j.io.pagecache.tracing.FileFlushEvent;
import org.neo4j.kernel.impl.transaction.log.checkpoint.CheckPointerImpl;
import org.neo4j.kernel.impl.transaction.log.checkpoint.SimpleTriggerInfo;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;
import org.neo4j.logging.AssertableLogProvider;
import org.neo4j.logging.LogAssertions;
import org.neo4j.logging.internal.DatabaseLogService;
import org.neo4j.logging.internal.LogService;
import org.neo4j.memory.GlobalMemoryGroupTracker;
import org.neo4j.memory.MemoryGroup;
import org.neo4j.memory.MemoryPools;
import org.neo4j.memory.MemoryTracker;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;
import org.neo4j.test.extension.DbmsExtension;
import org.neo4j.test.extension.ExtensionCallback;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.pagecache.PageCacheSupportExtension;
import org.neo4j.test.extension.testdirectory.TestDirectoryExtension;
import org.neo4j.test.utils.TestDirectory;

@TestDirectoryExtension
@DbmsExtension(configurationCallback = "configure")
class DatabaseIT {
    @RegisterExtension
    static PageCacheSupportExtension pageCacheExtension = new PageCacheSupportExtension();

    @Inject
    private FileSystemAbstraction fs;

    @Inject
    private TestDirectory directory;

    @Inject
    private DatabaseLayout databaseLayout;

    @Inject
    private Database database;

    @Inject
    private MemoryPools memoryPools;

    @Inject
    private DatabaseManagementService dbms;

    private PageCacheWrapper pageCacheWrapper;
    private final AssertableLogProvider logProvider = new AssertableLogProvider();

    @ExtensionCallback
    void configure(TestDatabaseManagementServiceBuilder builder) {
        pageCacheWrapper = new PageCacheWrapper(pageCacheExtension.getPageCache(fs));
        builder.setInternalLogProvider(logProvider).setExternalDependencies(dependenciesOf(pageCacheWrapper));
    }

    @AfterEach
    void tearDown() {
        dbms.shutdown();
        pageCacheWrapper.close();
    }

    @Test
    void shutdownOfDatabaseShouldFlushWithoutAnyIOLimitations() {
        pageCacheWrapper.disabledIOController.set(true);

        assertThat(pageCacheWrapper.ioControllerChecks.get()).isZero();
        assertDoesNotThrow(() -> database.stop());

        assertThat(pageCacheWrapper.ioControllerChecks.get()).isPositive();
    }

    @Test
    void dropDataOfNotStartedDatabase() {
        database.stop();

        assertNotEquals(databaseLayout.databaseDirectory(), databaseLayout.getTransactionLogsDirectory());
        assertTrue(fs.fileExists(databaseLayout.databaseDirectory()));
        assertTrue(fs.fileExists(databaseLayout.getTransactionLogsDirectory()));

        database.drop();

        assertFalse(fs.fileExists(databaseLayout.databaseDirectory()));
        assertFalse(fs.fileExists(databaseLayout.getTransactionLogsDirectory()));
        assertFalse(fs.fileExists(databaseLayout.getScriptDirectory()));
    }

    @Test
    void noPageCacheFlushOnDatabaseDrop() {
        database.start();
        int flushesBefore = pageCacheWrapper.getFlushes();

        database.drop();

        assertEquals(flushesBefore, pageCacheWrapper.getFlushes());
    }

    @Test
    void removeDatabaseDataAndLogsOnDrop() {
        assertNotEquals(databaseLayout.databaseDirectory(), databaseLayout.getTransactionLogsDirectory());
        assertTrue(fs.fileExists(databaseLayout.databaseDirectory()));
        assertTrue(fs.fileExists(databaseLayout.getTransactionLogsDirectory()));

        database.drop();

        assertFalse(fs.fileExists(databaseLayout.databaseDirectory()));
        assertFalse(fs.fileExists(databaseLayout.getTransactionLogsDirectory()));
        assertFalse(fs.fileExists(databaseLayout.getScriptDirectory()));
    }

    @Test
    void flushDatabaseDataOnStop() {
        String logPrefix = database.getNamedDatabaseId().logPrefix();
        int flushesBeforeClose = pageCacheWrapper.getFileFlushes();

        database.stop();

        assertNotEquals(flushesBeforeClose, pageCacheWrapper.getFileFlushes());
        LogAssertions.assertThat(logProvider)
                .forClass(Database.class)
                .forLevel(INFO)
                .containsMessages(
                        format("[%s] Waiting for closing transactions.", logPrefix),
                        format("[%s] All transactions are closed.", logPrefix));
    }

    @Test
    void flushOfThePageCacheHappensOnlyOnceDuringShutdown() throws Throwable {
        int databaseFiles =
                (int) database.getStoreFileListing().builder().build().stream().count();
        int flushesBefore = pageCacheWrapper.getFlushes();
        int filesFlushesBefore = pageCacheWrapper.getFileFlushes();
        database.stop();

        assertEquals(flushesBefore, pageCacheWrapper.getFlushes());
        assertThat(pageCacheWrapper.getFileFlushes()).isGreaterThanOrEqualTo(filesFlushesBefore + databaseFiles);
    }

    @Test
    void flushOfThePageCacheOnShutdownDoesNotHappenIfTheDbIsUnhealthy() throws Throwable {
        var databaseHealth = database.getDatabaseHealth();
        databaseHealth.panic(new Throwable("Critical failure"));
        int fileFlushesBefore = pageCacheWrapper.getFileFlushes();
        int databaseFiles = (int) database.getStoreFileListing().builder().excludeLogFiles().build().stream()
                .count();

        database.stop();

        assertThat(pageCacheWrapper.getFileFlushes()).isLessThan(fileFlushesBefore + databaseFiles);
    }

    @Test
    void logModuleSetUpError() {
        var exception = new RuntimeException("StartupError");

        database.stop();
        database.init();

        database.getLife().add(LifecycleAdapter.onStart(() -> {
            throw exception;
        }));

        var e = assertThrows(Exception.class, () -> database.start());
        assertThat(e).hasRootCause(exception);

        LogAssertions.assertThat(logProvider)
                .forClass(Database.class)
                .forLevel(WARN)
                .containsMessageWithException(
                        "Exception occurred while starting the database. Trying to stop already started components.",
                        e.getCause());
    }

    @Test
    void shouldAlwaysShutdownLifeEvenWhenSomeComponentFailing() {
        var expectedException = new RuntimeException("Failure");
        var life = database.getLife();
        var availabilityGuard = database.getDatabaseAvailabilityGuard();
        life.add(LifecycleAdapter.onShutdown(() -> {
            throw expectedException;
        }));
        var e = assertThrows(Throwable.class, () -> database.stop());
        assertThat(e).hasCause(expectedException);
        assertFalse(availabilityGuard.isAvailable());
    }

    @Test
    void shouldHaveDatabaseLogServiceInDependencyResolver() {
        var logService = database.getDependencyResolver().resolveDependency(LogService.class);
        assertEquals(database.getLogService(), logService);
        assertThat(logService).isInstanceOf(DatabaseLogService.class);
    }

    @Test
    void stopShutdownMustOnlyReleaseMemoryOnce() throws Exception {
        MemoryTracker otherMemoryTracker = getOtherMemoryTracker();

        long beforeStop = otherMemoryTracker.usedNativeMemory();

        database.stop();
        long afterStop = otherMemoryTracker.usedNativeMemory();
        assertThat(afterStop).isLessThan(beforeStop);

        database.shutdown();
        long afterShutdown = otherMemoryTracker.usedNativeMemory();
        assertEquals(afterShutdown, afterStop);
    }

    @Test
    void shutdownShutdownMustOnlyReleaseMemoryOnce() throws Exception {
        MemoryTracker otherMemoryTracker = getOtherMemoryTracker();

        long beforeShutdown = otherMemoryTracker.usedNativeMemory();

        database.shutdown();
        long afterFirstShutdown = otherMemoryTracker.usedNativeMemory();
        assertThat(afterFirstShutdown).isLessThan(beforeShutdown);

        database.shutdown();
        long afterSecondShutdown = otherMemoryTracker.usedNativeMemory();
        assertEquals(afterSecondShutdown, afterFirstShutdown);
    }

    @Test
    void shutdownStopMustOnlyReleaseMemoryOnce() throws Exception {
        MemoryTracker otherMemoryTracker = getOtherMemoryTracker();

        long beforeShutdown = otherMemoryTracker.usedNativeMemory();

        database.shutdown();
        long afterShutdown = otherMemoryTracker.usedNativeMemory();
        assertThat(afterShutdown).isLessThan(beforeShutdown);

        database.stop();
        long afterStop = otherMemoryTracker.usedNativeMemory();
        assertEquals(afterStop, afterShutdown);
    }

    @Test
    void shouldFlushDatabaseFilesOnCheckpoint() throws Exception {
        // Given
        DependencyResolver dependencyResolver = database.getDependencyResolver();
        Map<PageFileWrapper, Integer> flushCounts = new HashMap<>();
        Set<PageFileWrapper> mappedFiles = pageCacheWrapper.getMappedFilesForDatabase(
                database.getNamedDatabaseId().name());
        for (PageFileWrapper mappedFile : mappedFiles) {
            flushCounts.put(mappedFile, mappedFile.getLocalFlushCount());
        }
        // When
        CheckPointerImpl checkPointer = dependencyResolver.resolveDependency(CheckPointerImpl.class);
        checkPointer.forceCheckPoint(new SimpleTriggerInfo("test"));
        // Then
        for (PageFileWrapper mappedFile : mappedFiles) {
            int numFlushesDuringCheckpoint = mappedFile.getLocalFlushCount() - flushCounts.get(mappedFile);
            assertThat(numFlushesDuringCheckpoint)
                    .as(mappedFile.path().getFileName() + " should flush")
                    .isEqualTo(numberOfExpectedFlushesAtCheckpoint(mappedFile.path()));
        }
    }

    @Test
    void shouldFlushAllFilesOnShutdown() {
        // Given
        Map<PageFileWrapper, Integer> flushCounts = new HashMap<>();
        Set<PageFileWrapper> mappedFiles = pageCacheWrapper.getMappedFiles();
        for (PageFileWrapper mappedFile : mappedFiles) {
            flushCounts.put(mappedFile, mappedFile.getLocalFlushCount());
        }
        // When
        dbms.shutdown();
        // Then
        for (PageFileWrapper mappedFile : mappedFiles) {
            int numFlushesDuringShutdown = mappedFile.getLocalFlushCount() - flushCounts.get(mappedFile);
            assertThat(numFlushesDuringShutdown)
                    .as(mappedFile.path().getFileName() + " should flush on shutdown")
                    .isPositive();
        }
    }

    private int numberOfExpectedFlushesAtCheckpoint(Path storeFile) {
        PageCache pageCache = database.getDependencyResolver().resolveDependency(PageCache.class);
        MutableSet<OpenOption> openOptions = mutable.empty();
        try {
            PagedFile pagedFile = pageCache.getExistingMapping(storeFile).orElseThrow();
            try (PageCursor cursor = pagedFile.io(
                    0, PagedFile.PF_SHARED_READ_LOCK | PagedFile.PF_NO_FAULT, CursorContext.NULL_CONTEXT)) {
                if (Objects.equals(cursor.getByteOrder(), ByteOrder.BIG_ENDIAN)) {
                    openOptions.add(PageCacheOpenOptions.BIG_ENDIAN);
                }
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        try {
            GBPTreeVisitor.Adaptor<?, ?, ?> visitor = new GBPTreeVisitor.Adaptor<>();
            String dbName = "CheckIfGBPTree";
            // If we can visit both Meta and State (pages 0,1,2) without Exception we can assume it's a GBPTree
            GBPTreeStructure.visitMeta(
                    pageCacheWrapper,
                    storeFile,
                    visitor,
                    dbName,
                    CursorContext.NULL_CONTEXT,
                    openOptions.toImmutable());
            GBPTreeStructure.visitState(
                    pageCacheWrapper,
                    storeFile,
                    visitor,
                    dbName,
                    CursorContext.NULL_CONTEXT,
                    openOptions.toImmutable());
            return 3; // GPBTree files flush 3 times during checkpoint
        } catch (Exception e) {
            return 1; // Other store files flushes just once
        }
    }

    private MemoryTracker getOtherMemoryTracker() {
        for (GlobalMemoryGroupTracker pool : memoryPools.getPools()) {
            if (pool.group().equals(MemoryGroup.OTHER)) {
                return pool.getPoolMemoryTracker();
            }
        }
        throw new RuntimeException("Could not find memory tracker for group " + MemoryGroup.OTHER);
    }

    private static class PageCacheWrapper extends DelegatingPageCache {
        private final AtomicInteger flushes = new AtomicInteger();
        private final AtomicInteger fileFlushes = new AtomicInteger();
        private final AtomicInteger ioControllerChecks = new AtomicInteger();
        private final AtomicBoolean disabledIOController = new AtomicBoolean();
        private final Set<PageFileWrapper> mappedFiles = ConcurrentHashMap.newKeySet();

        PageCacheWrapper(PageCache delegate) {
            super(delegate);
        }

        @Override
        public PagedFile map(
                Path path,
                int pageSize,
                String databaseName,
                ImmutableSet<OpenOption> openOptions,
                IOController ioController,
                EvictionBouncer evictionBouncer,
                VersionStorage versionStorage)
                throws IOException {
            PageFileWrapper pageFileWrapper = new PageFileWrapper(
                    super.map(path, pageSize, databaseName, openOptions, ioController, evictionBouncer, versionStorage),
                    fileFlushes,
                    ioController,
                    disabledIOController,
                    ioControllerChecks,
                    mappedFiles::remove);
            mappedFiles.add(pageFileWrapper);
            return pageFileWrapper;
        }

        @Override
        public void flushAndForce(DatabaseFlushEvent flushEvent) throws IOException {
            flushes.incrementAndGet();
            super.flushAndForce(flushEvent);
        }

        Set<PageFileWrapper> getMappedFiles() {
            return getMappedFilesForDatabase("");
        }

        Set<PageFileWrapper> getMappedFilesForDatabase(String databaseName) {
            return mappedFiles.stream()
                    .filter(pageFileWrapper -> pageFileWrapper.getDatabaseName().contains(databaseName))
                    .collect(Collectors.toSet());
        }

        public int getFlushes() {
            return flushes.get();
        }

        public int getFileFlushes() {
            return fileFlushes.get();
        }
    }

    private static class PageFileWrapper extends DelegatingPagedFile {
        private final AtomicInteger globalFlushCounter;
        private final AtomicInteger fileLocalFlushCounter = new AtomicInteger();
        private final IOController ioController;
        private final AtomicBoolean disabledIOController;
        private final AtomicInteger ioControllerChecks;
        private final Consumer<PageFileWrapper> onClose;

        PageFileWrapper(
                PagedFile delegate,
                AtomicInteger globalFlushCounter,
                IOController ioController,
                AtomicBoolean disabledIOController,
                AtomicInteger ioControllerChecks,
                Consumer<PageFileWrapper> onClose) {
            super(delegate);
            this.globalFlushCounter = globalFlushCounter;
            this.ioController = ioController;
            this.disabledIOController = disabledIOController;
            this.ioControllerChecks = ioControllerChecks;
            this.onClose = onClose;
        }

        @Override
        public void flushAndForce(FileFlushEvent flushEvent) throws IOException {
            if (disabledIOController.get()) {
                assertFalse(ioController.isEnabled());
                ioControllerChecks.incrementAndGet();
            }
            globalFlushCounter.incrementAndGet();
            fileLocalFlushCounter.incrementAndGet();
            super.flushAndForce(flushEvent);
        }

        @Override
        public void close() {
            super.close();
            onClose.accept(this);
        }

        int getLocalFlushCount() {
            return fileLocalFlushCounter.get();
        }
    }
}
