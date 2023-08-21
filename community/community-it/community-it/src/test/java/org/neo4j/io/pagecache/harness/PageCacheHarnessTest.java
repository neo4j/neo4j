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
package org.neo4j.io.pagecache.harness;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;
import static org.neo4j.io.pagecache.PagedFile.PF_SHARED_READ_LOCK;
import static org.neo4j.io.pagecache.PagedFile.PF_SHARED_WRITE_LOCK;
import static org.neo4j.io.pagecache.context.CursorContext.NULL_CONTEXT;
import static org.neo4j.io.pagecache.randomharness.Command.FlushCache;
import static org.neo4j.io.pagecache.randomharness.Command.FlushFile;
import static org.neo4j.io.pagecache.randomharness.Command.MapFile;
import static org.neo4j.io.pagecache.randomharness.Command.ReadMulti;
import static org.neo4j.io.pagecache.randomharness.Command.ReadRecord;
import static org.neo4j.io.pagecache.randomharness.Command.Touch;
import static org.neo4j.io.pagecache.randomharness.Command.UnmapFile;
import static org.neo4j.io.pagecache.randomharness.Command.WriteMulti;
import static org.neo4j.io.pagecache.randomharness.Command.WriteRecord;
import static org.neo4j.test.extension.ExecutionSharedContext.SHARED_RESOURCE;

import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.eclipse.collections.api.factory.Sets;
import org.eclipse.collections.api.set.ImmutableSet;
import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.ResourceLock;
import org.neo4j.io.fs.StoreChannel;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.PageCacheTestSupport;
import org.neo4j.io.pagecache.randomharness.Command;
import org.neo4j.io.pagecache.randomharness.PageCountRecordFormat;
import org.neo4j.io.pagecache.randomharness.Phase;
import org.neo4j.io.pagecache.randomharness.RandomPageCacheTestHarness;
import org.neo4j.io.pagecache.randomharness.RecordFormat;
import org.neo4j.io.pagecache.randomharness.StandardRecordFormat;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.testdirectory.TestDirectoryExtension;
import org.neo4j.test.utils.TestDirectory;

@TestDirectoryExtension
@ResourceLock(SHARED_RESOURCE)
abstract class PageCacheHarnessTest<T extends PageCache> extends PageCacheTestSupport<T> {
    @Inject
    public TestDirectory directory;

    ImmutableSet<OpenOption> getOpenOptions() {
        return Sets.immutable.empty();
    }

    List<Command> additionalDisabledCommands() {
        return List.of();
    }

    @RepeatedTest(10)
    void readsAndWritesMustBeMutuallyConsistent() throws Exception {
        int filePageCount = 100;
        try (RandomPageCacheTestHarness harness = new RandomPageCacheTestHarness()) {
            harness.disableCommands(FlushCache, FlushFile, MapFile, UnmapFile);
            additionalDisabledCommands().forEach(harness::disableCommands);
            harness.setCommandProbabilityFactor(ReadRecord, 0.5);
            harness.setCommandProbabilityFactor(WriteRecord, 0.5);
            harness.setConcurrencyLevel(8);
            harness.setFilePageCount(filePageCount);
            harness.setInitialMappedFiles(1);
            harness.setFileSystem(fs);
            harness.setVerification(filesAreCorrectlyWrittenVerification(new StandardRecordFormat(), filePageCount));
            harness.setBasePath(directory.directory("readsAndWritesMustBeMutuallyConsistent"));
            harness.setOpenOptions(getOpenOptions());
            harness.run(SEMI_LONG_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS);
        }
    }

    @Test
    void concurrentPageFaultingMustNotPutInterleavedDataIntoPages() throws Exception {
        int filePageCount = 11;
        RecordFormat recordFormat = new PageCountRecordFormat();
        try (RandomPageCacheTestHarness harness = new RandomPageCacheTestHarness()) {
            harness.setConcurrencyLevel(11);
            harness.setUseAdversarialIO(false);
            harness.setCachePageCount(3);
            harness.setFilePageCount(filePageCount);
            harness.setInitialMappedFiles(1);
            harness.setCommandCount(10000);
            harness.setRecordFormat(recordFormat);
            harness.setFileSystem(fs);
            harness.setBasePath(directory.directory("concurrentPageFaultingMustNotPutInterleavedDataIntoPages"));
            harness.setOpenOptions(getOpenOptions());
            harness.disableCommands(FlushCache, FlushFile, MapFile, UnmapFile, WriteRecord, WriteMulti);
            additionalDisabledCommands().forEach(harness::disableCommands);
            harness.setPreparation((cache, fs, filesTouched) -> {
                Path file = filesTouched.iterator().next();
                try (var pf = cache.map(file, cache.pageSize(), DEFAULT_DATABASE_NAME, getOpenOptions());
                        var cursor = pf.io(0, PF_SHARED_WRITE_LOCK, NULL_CONTEXT)) {
                    for (int pageId = 0; pageId < filePageCount; pageId++) {
                        cursor.next();
                        recordFormat.fillWithRecords(cursor);
                    }
                }
            });

            harness.run(LONG_TIMEOUT_MILLIS, MILLISECONDS);
        }
    }

    @Test
    void concurrentFlushingMustNotPutInterleavedDataIntoFile() throws Exception {
        RecordFormat recordFormat = new StandardRecordFormat();
        int filePageCount = 2_000;
        try (RandomPageCacheTestHarness harness = new RandomPageCacheTestHarness()) {
            harness.setConcurrencyLevel(16);
            harness.setUseAdversarialIO(false);
            harness.setCachePageCount(filePageCount / 2);
            harness.setFilePageCount(filePageCount);
            harness.setInitialMappedFiles(3);
            harness.setCommandCount(15_000);
            harness.setFileSystem(fs);
            harness.setBasePath(directory.directory("concurrentFlushingMustNotPutInterleavedDataIntoFile"));
            harness.setOpenOptions(getOpenOptions());
            harness.disableCommands(MapFile, UnmapFile, ReadRecord, ReadMulti);
            additionalDisabledCommands().forEach(harness::disableCommands);
            harness.setVerification(filesAreCorrectlyWrittenVerification(recordFormat, filePageCount));

            harness.run(LONG_TIMEOUT_MILLIS, MILLISECONDS);
        }
    }

    @Test
    void concurrentFlushingWithMischiefMustNotPutInterleavedDataIntoFile() throws Exception {
        RecordFormat recordFormat = new StandardRecordFormat();
        int filePageCount = 2_000;
        try (RandomPageCacheTestHarness harness = new RandomPageCacheTestHarness()) {
            harness.setConcurrencyLevel(16);
            harness.setUseAdversarialIO(true);
            harness.setMischiefRate(0.5);
            harness.setFailureRate(0.0);
            harness.setErrorRate(0.0);
            harness.setCachePageCount(filePageCount / 2);
            harness.setFilePageCount(filePageCount);
            harness.setInitialMappedFiles(3);
            harness.setCommandCount(15_000);
            harness.setFileSystem(fs);
            harness.setBasePath(directory.directory("concurrentFlushingWithMischiefMustNotPutInterleavedDataIntoFile"));
            harness.setOpenOptions(getOpenOptions());
            harness.disableCommands(MapFile, UnmapFile, ReadRecord, ReadMulti);
            additionalDisabledCommands().forEach(harness::disableCommands);
            harness.setVerification(filesAreCorrectlyWrittenVerification(recordFormat, filePageCount));

            harness.run(LONG_TIMEOUT_MILLIS, MILLISECONDS);
        }
    }

    @Test
    void concurrentFlushingWithFailuresMustNotPutInterleavedDataIntoFile() throws Exception {
        RecordFormat recordFormat = new StandardRecordFormat();
        int filePageCount = 2_000;
        try (RandomPageCacheTestHarness harness = new RandomPageCacheTestHarness()) {
            harness.setConcurrencyLevel(16);
            harness.setUseAdversarialIO(true);
            harness.setMischiefRate(0.0);
            harness.setFailureRate(0.5);
            harness.setErrorRate(0.0);
            harness.setCachePageCount(filePageCount / 2);
            harness.setFilePageCount(filePageCount);
            harness.setInitialMappedFiles(3);
            harness.setCommandCount(15_000);
            harness.setFileSystem(fs);
            harness.setBasePath(directory.directory("concurrentFlushingWithFailuresMustNotPutInterleavedDataIntoFile"));
            harness.setOpenOptions(getOpenOptions());
            harness.disableCommands(MapFile, UnmapFile, ReadRecord, ReadMulti);
            additionalDisabledCommands().forEach(harness::disableCommands);
            harness.setVerification(filesAreCorrectlyWrittenVerification(recordFormat, filePageCount));

            harness.run(LONG_TIMEOUT_MILLIS, MILLISECONDS);
        }
    }

    @RepeatedTest(10)
    void readsWritesAndTouches() throws Exception {
        int filePageCount = 100;
        try (RandomPageCacheTestHarness harness = new RandomPageCacheTestHarness()) {
            harness.disableCommands(MapFile, UnmapFile);
            additionalDisabledCommands().forEach(harness::disableCommands);
            harness.setCommandProbabilityFactor(ReadRecord, 0.5);
            harness.setCommandProbabilityFactor(WriteRecord, 0.5);
            harness.setCommandProbabilityFactor(Touch, 0.5);
            harness.setConcurrencyLevel(8);
            harness.setFilePageCount(filePageCount);
            harness.setInitialMappedFiles(5);
            harness.setCachePageCount(filePageCount);
            harness.setFileSystem(fs);
            harness.setVerification(filesAreCorrectlyWrittenVerification(new StandardRecordFormat(), filePageCount));
            harness.setBasePath(directory.directory("readsWritesAndTouches"));
            harness.setOpenOptions(getOpenOptions());
            harness.run(SEMI_LONG_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS);
        }
    }

    private Phase filesAreCorrectlyWrittenVerification(final RecordFormat recordFormat, final int filePageCount) {
        return (cache, fs1, filesTouched) -> {
            for (Path file : filesTouched) {
                var openOptions = getOpenOptions();
                try (var pf = cache.map(file, cache.pageSize(), DEFAULT_DATABASE_NAME, openOptions);
                        var cursor = pf.io(0, PF_SHARED_READ_LOCK, NULL_CONTEXT)) {
                    for (int pageId = 0; pageId < filePageCount && cursor.next(); pageId++) {
                        try {
                            recordFormat.assertRecordsWrittenCorrectly(cursor);
                        } catch (Throwable th) {
                            th.addSuppressed(new Exception("pageId = " + pageId));
                            throw th;
                        }
                    }
                }
                var reservedBytes = cache.pageReservedBytes(openOptions);
                try (StoreChannel channel = fs1.read(file)) {
                    recordFormat.assertRecordsWrittenCorrectly(file, channel, reservedBytes);
                }
            }
        };
    }
}
