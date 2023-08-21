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

import static org.apache.commons.lang3.exception.ExceptionUtils.hasCause;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.eclipse.collections.api.factory.Sets.immutable;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.neo4j.internal.kernel.api.InternalIndexState.FAILED;
import static org.neo4j.internal.kernel.api.InternalIndexState.ONLINE;
import static org.neo4j.io.pagecache.context.CursorContext.NULL_CONTEXT;
import static org.neo4j.kernel.impl.api.index.PhaseTracker.nullInstance;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Random;
import org.apache.commons.lang3.RandomStringUtils;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.index.internal.gbptree.GBPTree;
import org.neo4j.index.internal.gbptree.GBPTreeBuilder;
import org.neo4j.index.internal.gbptree.Layout;
import org.neo4j.index.internal.gbptree.MetadataMismatchException;
import org.neo4j.internal.kernel.api.InternalIndexState;
import org.neo4j.io.fs.StoreChannel;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.PagedFile;
import org.neo4j.io.pagecache.tracing.DefaultPageCacheTracer;
import org.neo4j.kernel.api.index.IndexPopulator;
import org.neo4j.storageengine.api.IndexEntryUpdate;
import org.neo4j.test.extension.pagecache.PageCacheSupportExtension;
import org.neo4j.test.utils.PageCacheConfig;

abstract class IndexPopulatorTests<KEY, VALUE, LAYOUT extends Layout<KEY, VALUE>>
        extends IndexTestUtil<KEY, VALUE, LAYOUT> {
    IndexPopulator populator;

    @BeforeEach
    void setupPopulator() throws IOException {
        populator = createPopulator(pageCache);
    }

    abstract IndexPopulator createPopulator(PageCache pageCache) throws IOException;

    abstract byte failureByte();

    abstract byte populatingByte();

    abstract byte onlineByte();

    @Test
    void createShouldCreateFile() throws IOException {
        // given
        assertFileNotPresent();

        // when
        populator.create();

        // then
        assertFilePresent();
        populator.close(true, NULL_CONTEXT);
    }

    @Test
    void createShouldClearExistingFile() throws Exception {
        // given
        byte[] someBytes = fileWithContent();
        var storeFile = indexFiles.getStoreFile();
        assertThat(fs.fileExists(storeFile)).isTrue();
        assertThat(fs.getFileSize(storeFile)).isEqualTo(someBytes.length);

        // when
        populator.create();

        // then
        assertThat(fs.fileExists(storeFile)).isTrue();
        assertThat(fs.getFileSize(storeFile)).isEqualTo(0);

        populator.close(true, NULL_CONTEXT);
    }

    @Test
    void dropShouldDeleteExistingFile() throws IOException {
        // given
        populator.create();

        // when
        populator.drop();

        // then
        assertFileNotPresent();
    }

    @Test
    void dropShouldSucceedOnNonExistentFile() {
        // given
        assertFileNotPresent();

        // when
        populator.drop();

        // then
        assertFileNotPresent();
    }

    @Test
    void addShouldHandleEmptyCollection() throws Exception {
        // given
        populator.create();
        List<IndexEntryUpdate<?>> updates = Collections.emptyList();

        // when
        populator.add(updates, NULL_CONTEXT);
        populator.scanCompleted(nullInstance, populationWorkScheduler, NULL_CONTEXT);

        // then
        populator.close(true, NULL_CONTEXT);
    }

    @Test
    void successfulCloseMustCloseGBPTree() throws Exception {
        // given
        populator.create();
        Optional<PagedFile> existingMapping = pageCache.getExistingMapping(indexFiles.getStoreFile());
        if (existingMapping.isPresent()) {
            existingMapping.get().close();
        } else {
            fail("Expected underlying GBPTree to have a mapping for this file");
        }

        // when
        populator.close(true, NULL_CONTEXT);

        // then
        existingMapping = pageCache.getExistingMapping(indexFiles.getStoreFile());
        assertFalse(existingMapping.isPresent());
    }

    @Test
    void successfulCloseMustMarkIndexAsOnline() throws Exception {
        // given
        populator.create();

        // when
        populator.close(true, NULL_CONTEXT);

        // then
        assertHeader(ONLINE, null, false);
    }

    @Test
    void unsuccessfulCloseMustSucceedWithoutMarkAsFailed() throws IOException {
        // given
        populator.create();

        // then
        populator.close(false, NULL_CONTEXT);
    }

    @Test
    void unsuccessfulCloseMustCloseGBPTree() throws Exception {
        // given
        populator.create();
        Optional<PagedFile> existingMapping = pageCache.getExistingMapping(indexFiles.getStoreFile());
        if (existingMapping.isPresent()) {
            existingMapping.get().close();
        } else {
            fail("Expected underlying GBPTree to have a mapping for this file");
        }

        // when
        populator.close(false, NULL_CONTEXT);

        // then
        existingMapping = pageCache.getExistingMapping(indexFiles.getStoreFile());
        assertFalse(existingMapping.isPresent());
    }

    @Test
    void unsuccessfulCloseMustNotMarkIndexAsOnline() throws Exception {
        // given
        populator.create();

        // when
        populator.close(false, NULL_CONTEXT);

        // then
        assertNoHeader();
    }

    @Test
    void closeMustWriteFailureMessageAfterMarkedAsFailed() throws Exception {
        // given
        populator.create();

        // when
        String failureMessage = "Fly, you fools!";
        populator.markAsFailed(failureMessage);
        populator.close(false, NULL_CONTEXT);

        // then
        assertHeader(FAILED, failureMessage, false);
    }

    @Test
    void closeMustWriteFailureMessageAfterMarkedAsFailedWithLongMessage() throws Exception {
        // given
        populator.create();

        // when
        String failureMessage = longString(pageCache.pageSize());
        populator.markAsFailed(failureMessage);
        populator.close(false, NULL_CONTEXT);

        // then
        assertHeader(FAILED, failureMessage, true);
    }

    @Test
    void successfulCloseMustThrowIfMarkedAsFailed() throws IOException {
        // given
        populator.create();

        // when
        populator.markAsFailed("");

        // then
        var e = assertThrows(RuntimeException.class, () -> populator.close(true, NULL_CONTEXT));
        assertTrue(
                hasCause(e, IllegalStateException.class), "Expected cause to contain " + IllegalStateException.class);
        populator.close(false, NULL_CONTEXT);
    }

    @Test
    void dropMustSucceedAfterSuccessfulClose() throws IOException {
        // given
        populator.create();
        populator.close(true, NULL_CONTEXT);

        // when
        populator.drop();

        // then
        assertFileNotPresent();
    }

    @Test
    void dropMustSucceedAfterUnsuccessfulClose() throws IOException {
        // given
        populator.create();
        populator.close(false, NULL_CONTEXT);

        // when
        populator.drop();

        // then
        assertFileNotPresent();
    }

    @Test
    void dropShouldNotFlushContent() throws IOException {
        // given
        DefaultPageCacheTracer tracer = new DefaultPageCacheTracer();
        try (PageCache pageCache = PageCacheSupportExtension.getPageCache(
                fs, PageCacheConfig.config().withTracer(tracer))) {
            populator = createPopulator(pageCache);
            populator.create();
            long preDrop = tracer.flushes();

            // when
            populator.drop();

            // then
            long postDrop = tracer.flushes();
            assertEquals(preDrop, postDrop);
        }
    }

    @Test
    void successfulCloseMustThrowWithoutPriorSuccessfulCreate() {
        // given
        assertFileNotPresent();

        // when
        var e = assertThrows(RuntimeException.class, () -> populator.close(true, NULL_CONTEXT));
        assertTrue(
                hasCause(e, IllegalStateException.class), "Expected cause to contain " + IllegalStateException.class);
    }

    @Test
    void unsuccessfulCloseMustSucceedWithoutSuccessfulPriorCreate() throws Exception {
        // given
        assertFileNotPresent();
        String failureMessage = "There is no spoon";
        populator.markAsFailed(failureMessage);

        // when
        populator.close(false, NULL_CONTEXT);

        // then
        assertHeader(FAILED, failureMessage, false);
    }

    @Test
    void successfulCloseMustThrowAfterDrop() throws IOException {
        // given
        populator.create();

        // when
        populator.drop();

        // then
        var e = assertThrows(RuntimeException.class, () -> populator.close(true, NULL_CONTEXT));
        assertTrue(
                hasCause(e, IllegalStateException.class), "Expected cause to contain " + IllegalStateException.class);
    }

    @Test
    void unsuccessfulCloseMustThrowAfterDrop() throws IOException {
        // given
        populator.create();

        // when
        populator.drop();

        // then
        var e = assertThrows(RuntimeException.class, () -> populator.close(false, NULL_CONTEXT));
        assertTrue(
                hasCause(e, IllegalStateException.class), "Expected cause to contain " + IllegalStateException.class);
    }

    private void assertNoHeader() {
        NativeIndexHeaderReader headerReader = new NativeIndexHeaderReader(failureByte());
        var e = catchThrowable(() -> GBPTree.readHeader(
                pageCache, indexFiles.getStoreFile(), headerReader, "db", NULL_CONTEXT, immutable.empty()));
        assertThat(e).isInstanceOf(MetadataMismatchException.class);
    }

    private void assertHeader(InternalIndexState expectedState, String failureMessage, boolean messageTruncated)
            throws IOException {
        NativeIndexHeaderReader headerReader = new NativeIndexHeaderReader(failureByte());
        try (GBPTree<KEY, VALUE> ignored = new GBPTreeBuilder<>(pageCache, fs, indexFiles.getStoreFile(), layout)
                .with(headerReader)
                .build()) {
            switch (expectedState) {
                case ONLINE:
                    assertEquals(
                            onlineByte(),
                            headerReader.state,
                            "Index was not marked as online when expected not to be.");
                    assertNull(
                            headerReader.failureMessage, "Expected failure message to be null when marked as online.");
                    break;
                case FAILED:
                    assertEquals(
                            failureByte(), headerReader.state, "Index was marked as online when expected not to be.");
                    if (messageTruncated) {
                        assertTrue(headerReader.failureMessage.length() < failureMessage.length());
                        assertTrue(failureMessage.startsWith(headerReader.failureMessage));
                    } else {
                        assertEquals(failureMessage, headerReader.failureMessage);
                    }
                    break;
                case POPULATING:
                    assertEquals(
                            populatingByte(),
                            headerReader.state,
                            "Index was not left as populating when expected to be.");
                    assertNull(
                            headerReader.failureMessage,
                            "Expected failure message to be null when marked as populating.");
                    break;
                default:
                    throw new UnsupportedOperationException("Unexpected index state " + expectedState);
            }
        }
    }

    private static String longString(int length) {
        return RandomStringUtils.random(length, true, true);
    }

    private byte[] fileWithContent() throws IOException {
        int size = 1000;
        indexFiles.ensureDirectoryExist();
        try (StoreChannel storeChannel = fs.write(indexFiles.getStoreFile())) {
            byte[] someBytes = new byte[size];
            new Random().nextBytes(someBytes);
            storeChannel.writeAll(ByteBuffer.wrap(someBytes));
            return someBytes;
        }
    }
}
