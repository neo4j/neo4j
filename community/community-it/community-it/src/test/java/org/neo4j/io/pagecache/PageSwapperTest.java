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
package org.neo4j.io.pagecache;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assumptions.assumeThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.io.ByteUnit.KibiByte;
import static org.neo4j.io.pagecache.IOController.DISABLED;
import static org.neo4j.io.pagecache.impl.muninn.EvictionBouncer.ALWAYS_ALLOW;

import java.io.IOException;
import java.nio.channels.ClosedChannelException;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.DisabledOnOs;
import org.junit.jupiter.api.condition.EnabledOnOs;
import org.junit.jupiter.api.condition.OS;
import org.junit.jupiter.api.parallel.Isolated;
import org.neo4j.internal.unsafe.UnsafeUtil;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.mem.MemoryAllocator;
import org.neo4j.io.pagecache.impl.muninn.SwapperSet;
import org.neo4j.memory.LocalMemoryTracker;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.testdirectory.TestDirectoryExtension;
import org.neo4j.test.utils.TestDirectory;
import org.neo4j.util.concurrent.Futures;

@TestDirectoryExtension
@Isolated
public abstract class PageSwapperTest {
    @Inject
    protected TestDirectory testDir;

    public static final long X = 0xcafebabedeadbeefL;
    public static final long Y = X ^ (X << 1);
    public static final int Z = 0xfefefefe;

    protected static final PageEvictionCallback NO_CALLBACK = filePageId -> {};

    public static int RESERVED_BYTES;
    public static int PAYLOAD_SIZE;
    public static int cachePageSize;
    private final ConcurrentLinkedQueue<PageSwapper> openedSwappers = new ConcurrentLinkedQueue<>();
    private final MemoryAllocator mman =
            MemoryAllocator.createAllocator(KibiByte.toBytes(32), new LocalMemoryTracker());
    private final SwapperSet swapperSet = new SwapperSet();

    protected abstract PageSwapperFactory swapperFactory(FileSystemAbstraction fileSystem);

    protected abstract void mkdirs(Path dir) throws IOException;

    @BeforeAll
    static void beforeAll() {
        RESERVED_BYTES = 0;
        PAYLOAD_SIZE = 32;
        cachePageSize = PAYLOAD_SIZE + RESERVED_BYTES;
    }

    @BeforeEach
    @AfterEach
    void clearStrayInterrupts() {
        Thread.interrupted();
    }

    @AfterEach
    void closeOpenedPageSwappers() throws Exception {
        Exception exception = null;
        PageSwapper swapper;

        while ((swapper = openedSwappers.poll()) != null) {
            try {
                swapper.close();
            } catch (IOException e) {
                if (exception == null) {
                    exception = e;
                } else {
                    exception.addSuppressed(e);
                }
            }
        }

        if (exception != null) {
            throw exception;
        }
    }

    protected abstract FileSystemAbstraction getFs();

    @Test
    void readMustNotSwallowInterrupts() throws Exception {
        Path file = file("a");

        long page = createPage();
        putInt(page, 0, 1);
        PageSwapperFactory swapperFactory = createSwapperFactory(getFs());
        PageSwapper swapper = createSwapperAndFile(swapperFactory, file);

        assertThat(write(swapper, 0, page)).isEqualTo(sizeOfAsLong(page));
        putInt(page, 0, 0);
        Thread.currentThread().interrupt();

        assertThat(read(swapper, 0, page)).isEqualTo(sizeOfAsLong(page));
        assertTrue(Thread.currentThread().isInterrupted());
        assertThat(getInt(page, 0)).isEqualTo(1);

        assertThat(read(swapper, 0, page)).isEqualTo(sizeOfAsLong(page));
        assertTrue(Thread.currentThread().isInterrupted());
        assertThat(getInt(page, 0)).isEqualTo(1);
    }

    @Test
    void vectoredReadMustNotSwallowInterrupts() throws Exception {
        Path file = file("a");

        long page = createPage();
        putInt(page, 0, 1);
        PageSwapperFactory swapperFactory = createSwapperFactory(getFs());
        PageSwapper swapper = createSwapperAndFile(swapperFactory, file);

        assertThat(write(swapper, 0, page)).isEqualTo(sizeOfAsLong(page));
        putInt(page, 0, 0);
        Thread.currentThread().interrupt();

        assertThat(read(swapper, 0, new long[] {page}, new int[] {cachePageSize()}, 1))
                .isEqualTo(sizeOfAsLong(page));
        assertTrue(Thread.currentThread().isInterrupted());
        assertThat(getInt(page, 0)).isEqualTo(1);

        assertThat(read(swapper, 0, new long[] {page}, new int[] {cachePageSize()}, 1))
                .isEqualTo(sizeOfAsLong(page));
        assertTrue(Thread.currentThread().isInterrupted());
        assertThat(getInt(page, 0)).isEqualTo(1);
    }

    @Test
    void writeMustNotSwallowInterrupts() throws Exception {
        Path file = file("a");

        long page = createPage();
        putInt(page, 0, 1);
        PageSwapperFactory swapperFactory = createSwapperFactory(getFs());
        PageSwapper swapper = createSwapperAndFile(swapperFactory, file);

        Thread.currentThread().interrupt();

        assertThat(write(swapper, 0, page)).isEqualTo(sizeOfAsLong(page));
        assertTrue(Thread.currentThread().isInterrupted());

        putInt(page, 0, 0);
        assertThat(read(swapper, 0, page)).isEqualTo(sizeOfAsLong(page));
        assertThat(getInt(page, 0)).isEqualTo(1);

        assertThat(write(swapper, 0, page)).isEqualTo(sizeOfAsLong(page));
        assertTrue(Thread.currentThread().isInterrupted());

        putInt(page, 0, 0);
        assertThat(read(swapper, 0, page)).isEqualTo(sizeOfAsLong(page));
        assertThat(getInt(page, 0)).isEqualTo(1);
    }

    @Test
    void vectoredWriteMustNotSwallowInterrupts() throws Exception {
        Path file = file("a");

        long page = createPage();
        putInt(page, 0, 1);
        PageSwapperFactory swapperFactory = createSwapperFactory(getFs());
        PageSwapper swapper = createSwapperAndFile(swapperFactory, file);

        Thread.currentThread().interrupt();

        assertThat(write(swapper, 0, new long[] {page}, new int[] {cachePageSize()}, 1, 1))
                .isEqualTo(sizeOfAsLong(page));
        assertTrue(Thread.currentThread().isInterrupted());

        putInt(page, 0, 0);
        assertThat(read(swapper, 0, page)).isEqualTo(sizeOfAsLong(page));
        assertThat(getInt(page, 0)).isEqualTo(1);

        assertThat(write(swapper, 0, new long[] {page}, new int[] {cachePageSize()}, 1, 1))
                .isEqualTo(sizeOfAsLong(page));
        assertTrue(Thread.currentThread().isInterrupted());

        putInt(page, 0, 0);
        assertThat(read(swapper, 0, page)).isEqualTo(sizeOfAsLong(page));
        assertThat(getInt(page, 0)).isEqualTo(1);
    }

    @Test
    void forcingMustNotSwallowInterrupts() throws Exception {
        Path file = file("a");

        PageSwapperFactory swapperFactory = createSwapperFactory(getFs());
        PageSwapper swapper = createSwapperAndFile(swapperFactory, file);

        Thread.currentThread().interrupt();
        swapper.force();
        assertTrue(Thread.currentThread().isInterrupted());
    }

    @Test
    void mustReopenChannelWhenReadFailsWithAsynchronousCloseException() throws Exception {
        Path file = file("a");
        PageSwapperFactory swapperFactory = createSwapperFactory(getFs());
        PageSwapper swapper = createSwapperAndFile(swapperFactory, file);

        long page = createPage();
        putLong(page, 0, X);
        putLong(page, 8, Y);
        putInt(page, 16, Z);
        write(swapper, 0, page);

        Thread.currentThread().interrupt();

        read(swapper, 0, page);

        // Clear the interrupted flag and assert that it was still raised
        assertTrue(Thread.interrupted());

        assertThat(getLong(page, 0)).isEqualTo(X);
        assertThat(getLong(page, 8)).isEqualTo(Y);
        assertThat(getInt(page, 16)).isEqualTo(Z);

        // This must not throw because we should still have a usable channel
        swapper.force();
    }

    @Test
    void mustReopenChannelWhenVectoredReadFailsWithAsynchronousCloseException() throws Exception {
        Path file = file("a");
        PageSwapperFactory swapperFactory = createSwapperFactory(getFs());
        PageSwapper swapper = createSwapperAndFile(swapperFactory, file);

        long page = createPage();
        putLong(page, 0, X);
        putLong(page, 8, Y);
        putInt(page, 16, Z);
        write(swapper, 0, page);

        Thread.currentThread().interrupt();

        read(swapper, 0, new long[] {page}, new int[] {cachePageSize()}, 1);

        // Clear the interrupted flag and assert that it was still raised
        assertTrue(Thread.interrupted());

        assertThat(getLong(page, 0)).isEqualTo(X);
        assertThat(getLong(page, 8)).isEqualTo(Y);
        assertThat(getInt(page, 16)).isEqualTo(Z);

        // This must not throw because we should still have a usable channel
        swapper.force();
    }

    @Test
    void mustReopenChannelWhenWriteFailsWithAsynchronousCloseException() throws Exception {
        long page = createPage();
        putLong(page, 0, X);
        putLong(page, 8, Y);
        putInt(page, 16, Z);
        Path file = file("a");

        PageSwapperFactory swapperFactory = createSwapperFactory(getFs());
        PageSwapper swapper = createSwapperAndFile(swapperFactory, file);

        Thread.currentThread().interrupt();

        write(swapper, 0, page);

        // Clear the interrupted flag and assert that it was still raised
        assertTrue(Thread.interrupted());

        // This must not throw because we should still have a usable channel
        swapper.force();

        clear(page);
        read(swapper, 0, page);
        assertThat(getLong(page, 0)).isEqualTo(X);
        assertThat(getLong(page, 8)).isEqualTo(Y);
        assertThat(getInt(page, 16)).isEqualTo(Z);
    }

    @Test
    void mustReopenChannelWhenVectoredWriteFailsWithAsynchronousCloseException() throws Exception {
        long page = createPage();
        putLong(page, 0, X);
        putLong(page, 8, Y);
        putInt(page, 16, Z);
        Path file = file("a");

        PageSwapperFactory swapperFactory = createSwapperFactory(getFs());
        PageSwapper swapper = createSwapperAndFile(swapperFactory, file);

        Thread.currentThread().interrupt();

        write(swapper, 0, new long[] {page}, new int[] {cachePageSize()}, 1, 1);

        // Clear the interrupted flag and assert that it was still raised
        assertTrue(Thread.interrupted());

        // This must not throw because we should still have a usable channel
        swapper.force();

        clear(page);
        read(swapper, 0, page);
        assertThat(getLong(page, 0)).isEqualTo(X);
        assertThat(getLong(page, 8)).isEqualTo(Y);
        assertThat(getInt(page, 16)).isEqualTo(Z);
    }

    @Test
    void mustReopenChannelWhenForceFailsWithAsynchronousCloseException() throws Exception {
        Path file = file("a");

        PageSwapperFactory swapperFactory = createSwapperFactory(getFs());
        PageSwapper swapper = createSwapperAndFile(swapperFactory, file);

        for (int i = 0; i < 10; i++) {
            Thread.currentThread().interrupt();

            // This must not throw
            swapper.force();

            // Clear the interrupted flag and assert that it was still raised
            assertTrue(Thread.interrupted());
        }
    }

    @Test
    void readMustNotReopenExplicitlyClosedChannel() throws Exception {
        String filename = "a";
        Path file = file(filename);

        long page = createPage();
        PageSwapperFactory swapperFactory = createSwapperFactory(getFs());
        PageSwapper swapper = createSwapperAndFile(swapperFactory, file);
        write(swapper, 0, page);
        swapper.close();

        assertThrows(ClosedChannelException.class, () -> read(swapper, 0, page));
    }

    @Test
    void vectoredReadMustNotReopenExplicitlyClosedChannel() throws Exception {
        String filename = "a";
        Path file = file(filename);

        long page = createPage();
        PageSwapperFactory swapperFactory = createSwapperFactory(getFs());
        PageSwapper swapper = createSwapperAndFile(swapperFactory, file);
        write(swapper, 0, page);
        swapper.close();

        assertThrows(
                ClosedChannelException.class,
                () -> read(swapper, 0, new long[] {page}, new int[] {cachePageSize()}, 1));
    }

    @Test
    void writeMustNotReopenExplicitlyClosedChannel() throws Exception {
        Path file = file("a");

        long page = createPage();
        PageSwapperFactory swapperFactory = createSwapperFactory(getFs());
        PageSwapper swapper = createSwapperAndFile(swapperFactory, file);
        swapper.close();

        assertThrows(ClosedChannelException.class, () -> write(swapper, 0, page));
    }

    @Test
    void vectoredWriteMustNotReopenExplicitlyClosedChannel() throws Exception {
        Path file = file("a");

        long page = createPage();
        PageSwapperFactory swapperFactory = createSwapperFactory(getFs());
        PageSwapper swapper = createSwapperAndFile(swapperFactory, file);
        swapper.close();

        assertThrows(
                ClosedChannelException.class,
                () -> write(swapper, 0, new long[] {page}, new int[] {cachePageSize()}, 1, 1));
    }

    @Test
    void forceMustNotReopenExplicitlyClosedChannel() throws Exception {
        Path file = file("a");

        PageSwapperFactory swapperFactory = createSwapperFactory(getFs());
        PageSwapper swapper = createSwapperAndFile(swapperFactory, file);
        swapper.close();

        assertThrows(ClosedChannelException.class, swapper::force);
    }

    @Test
    void mustNotOverwriteDataInOtherFiles() throws Exception {
        Path fileA = file("a");
        Path fileB = file("b");
        PageSwapperFactory factory = createSwapperFactory(getFs());
        PageSwapper swapperA = createSwapperAndFile(factory, fileA);
        PageSwapper swapperB = createSwapperAndFile(factory, fileB);

        long page = createPage();
        clear(page);
        putLong(page, 0, X);
        write(swapperA, 0, page);
        putLong(page, 8, Y);
        write(swapperB, 0, page);

        clear(page);
        assertThat(getLong(page, 0)).isEqualTo(0L);
        assertThat(getLong(page, 8)).isEqualTo(0L);

        read(swapperA, 0, page);

        assertThat(getLong(page, 0)).isEqualTo(X);
        assertThat(getLong(page, 8)).isEqualTo(0L);
    }

    @Test
    void mustRunEvictionCallbackOnEviction() throws Exception {
        final AtomicLong callbackFilePageId = new AtomicLong();
        PageEvictionCallback callback = callbackFilePageId::set;
        Path file = file("file");
        PageSwapperFactory factory = createSwapperFactory(getFs());
        PageSwapper swapper = createSwapper(factory, file, cachePageSize(), callback, true, false);
        swapper.evicted(42);
        assertThat(callbackFilePageId.get()).isEqualTo(42L);
    }

    @Test
    void mustNotIssueEvictionCallbacksAfterSwapperHasBeenClosed() throws Exception {
        final AtomicBoolean gotCallback = new AtomicBoolean();
        PageEvictionCallback callback = filePageId -> gotCallback.set(true);
        Path file = file("file");
        PageSwapperFactory factory = createSwapperFactory(getFs());
        PageSwapper swapper = createSwapper(factory, file, cachePageSize(), callback, true, false);
        swapper.close();
        swapper.evicted(42);
        assertFalse(gotCallback.get());
    }

    @Test
    void mustThrowExceptionIfFileDoesNotExist() {
        PageSwapperFactory factory = createSwapperFactory(getFs());
        assertThrows(
                NoSuchFileException.class,
                () -> createSwapper(factory, file("does not exist"), cachePageSize(), NO_CALLBACK, false, false));
    }

    @Test
    void mustCreateNonExistingFileWithCreateFlag() throws Exception {
        PageSwapperFactory factory = createSwapperFactory(getFs());
        PageSwapper pageSwapper = createSwapperAndFile(factory, file("does not exist"));

        // After creating the file, we must also be able to read and write
        long page = createPage();
        putLong(page, 0, X);
        write(pageSwapper, 0, page);

        clear(page);
        read(pageSwapper, 0, page);

        assertThat(getLong(page, 0)).isEqualTo(X);
    }

    @Test
    void truncatedFilesMustBeEmpty() throws Exception {
        Path file = file("file");
        PageSwapperFactory factory = createSwapperFactory(getFs());
        PageSwapper swapper = createSwapperAndFile(factory, file);

        assertThat(swapper.getLastPageId()).isEqualTo(-1L);

        long page = createPage();
        putInt(page, 0, 0xcafebabe);
        write(swapper, 10, page);
        clear(page);
        read(swapper, 10, page);
        assertThat(getInt(page, 0)).isEqualTo(0xcafebabe);
        assertThat(swapper.getLastPageId()).isEqualTo(10L);

        swapper.close();
        swapper = createSwapper(factory, file, PAYLOAD_SIZE, NO_CALLBACK, false, false);
        clear(page);
        read(swapper, 10, page);
        assertThat(getInt(page, 0)).isEqualTo(0xcafebabe);
        assertThat(swapper.getLastPageId()).isEqualTo(10L);

        swapper.truncate();
        clear(page);
        read(swapper, 10, page);
        assertThat(getInt(page, 0)).isEqualTo(0);
        assertThat(swapper.getLastPageId()).isEqualTo(-1L);

        swapper.close();
        swapper = createSwapper(factory, file, PAYLOAD_SIZE, NO_CALLBACK, false, false);
        clear(page);
        read(swapper, 10, page);
        assertThat(getInt(page, 0)).isEqualTo(0);
        assertThat(swapper.getLastPageId()).isEqualTo(-1L);

        swapper.close();
    }

    @Test
    void positionedVectoredWriteMustFlushAllBuffersInOrder() throws Exception {
        Path file = file("file");
        PageSwapperFactory factory = createSwapperFactory(getFs());
        PageSwapper swapper = createSwapperAndFile(factory, file, 4);

        long pageA = createPage(4);
        long pageB = createPage(4);
        long pageC = createPage(4);
        long pageD = createPage(4);

        putInt(pageA, 0, 2);
        putInt(pageB, 0, 3);
        putInt(pageC, 0, 4);
        putInt(pageD, 0, 5);

        write(
                swapper,
                1,
                new long[] {pageA, pageB, pageC, pageD},
                new int[] {4 + RESERVED_BYTES, 4 + RESERVED_BYTES, 4 + RESERVED_BYTES, 4 + RESERVED_BYTES},
                4,
                4);

        long result = createPage(4);

        read(swapper, 0, result);
        assertThat(getInt(result, 0)).isEqualTo(0);
        putInt(result, 0, 0);
        assertThat(read(swapper, 1, result)).isEqualTo(4L + RESERVED_BYTES);
        assertThat(getInt(result, 0)).isEqualTo(2);
        putInt(result, 0, 0);
        assertThat(read(swapper, 2, result)).isEqualTo(4L + RESERVED_BYTES);
        assertThat(getInt(result, 0)).isEqualTo(3);
        putInt(result, 0, 0);
        assertThat(read(swapper, 3, result)).isEqualTo(4L + RESERVED_BYTES);
        assertThat(getInt(result, 0)).isEqualTo(4);
        putInt(result, 0, 0);
        assertThat(read(swapper, 4, result)).isEqualTo(4L + RESERVED_BYTES);
        assertThat(getInt(result, 0)).isEqualTo(5);
        putInt(result, 0, 0);
        assertThat(read(swapper, 5, result)).isEqualTo(0L);
        assertThat(getInt(result, 0)).isEqualTo(0);
    }

    @Test
    void positionedVectoredWriteMustFlushAllBuffersOfDifferentSizeInOrder() throws Exception {
        assumeThat(RESERVED_BYTES).isEqualTo(0);
        Path file = file("file");
        PageSwapperFactory factory = createSwapperFactory(getFs());
        PageSwapper swapper = createSwapperAndFile(factory, file, 4);

        long pageA = createPage(4);
        long pageB = createPage(8);
        long pageC = createPage(12);

        putInt(pageA, 0, 2);
        putInt(pageB, 0, 3);
        putInt(pageB, 4, 4);
        putInt(pageC, 0, 5);
        putInt(pageC, 4, 6);
        putInt(pageC, 8, 7);

        assertEquals(24, write(swapper, 0, new long[] {pageA, pageB, pageC}, new int[] {4, 8, 12}, 3, 6));

        long result = createPage(4);

        read(swapper, 0, result);
        assertThat(getInt(result, 0)).isEqualTo(2);
        putInt(result, 0, 0);
        assertThat(read(swapper, 1, result)).isEqualTo(4L);
        assertThat(getInt(result, 0)).isEqualTo(3);
        putInt(result, 0, 0);
        assertThat(read(swapper, 2, result)).isEqualTo(4L);
        assertThat(getInt(result, 0)).isEqualTo(4);
        putInt(result, 0, 0);
        assertThat(read(swapper, 3, result)).isEqualTo(4L);
        assertThat(getInt(result, 0)).isEqualTo(5);
        putInt(result, 0, 0);
        assertThat(read(swapper, 4, result)).isEqualTo(4L);
        assertThat(getInt(result, 0)).isEqualTo(6);
        putInt(result, 0, 0);
        assertThat(read(swapper, 5, result)).isEqualTo(4L);
        assertThat(getInt(result, 0)).isEqualTo(7);
    }

    @Test
    void positionedVectoredReadMustFillAllBuffersOfDifferentSizesInOrder() throws Exception {
        assumeThat(RESERVED_BYTES).isEqualTo(0);
        Path file = file("file");
        PageSwapperFactory factory = createSwapperFactory(getFs());
        PageSwapper swapper = createSwapperAndFile(factory, file, 4);

        long output = createPage();

        putInt(output, 0, 2);
        write(swapper, 1, output);
        putInt(output, 0, 3);
        write(swapper, 2, output);
        putInt(output, 0, 4);
        write(swapper, 3, output);
        putInt(output, 0, 5);
        write(swapper, 4, output);
        putInt(output, 0, 6);
        write(swapper, 5, output);
        putInt(output, 0, 7);
        write(swapper, 6, output);

        long pageA = createPage(4);
        long pageB = createPage(8);
        long pageC = createPage(12);

        // Read 4, 8 and 12 bytes into 3 buffers
        assertThat(read(swapper, 1, new long[] {pageA, pageB, pageC}, new int[] {4, 8, 12}, 3))
                .isEqualTo(4 + 8 + 12);

        assertThat(getInt(pageA, 0)).isEqualTo(2);
        assertThat(getInt(pageB, 0)).isEqualTo(3);
        assertThat(getInt(pageB, 4)).isEqualTo(4);
        assertThat(getInt(pageC, 0)).isEqualTo(5);
        assertThat(getInt(pageC, 4)).isEqualTo(6);
        assertThat(getInt(pageC, 8)).isEqualTo(7);
    }

    @Test
    void positionedVectoredReadMustFillAllBuffersInOrder() throws Exception {
        Path file = file("file");
        PageSwapperFactory factory = createSwapperFactory(getFs());
        PageSwapper swapper = createSwapperAndFile(factory, file, 4);

        long output = createPage();

        putInt(output, 0, 2);
        write(swapper, 1, output);
        putInt(output, 0, 3);
        write(swapper, 2, output);
        putInt(output, 0, 4);
        write(swapper, 3, output);
        putInt(output, 0, 5);
        write(swapper, 4, output);

        long pageA = createPage(4);
        long pageB = createPage(4);
        long pageC = createPage(4);
        long pageD = createPage(4);

        // Read 4 pages of 4 bytes each + reserved bytes
        assertThat(read(
                        swapper,
                        1,
                        new long[] {pageA, pageB, pageC, pageD},
                        new int[] {4 + RESERVED_BYTES, 4 + RESERVED_BYTES, 4 + RESERVED_BYTES, 4 + RESERVED_BYTES},
                        4))
                .isEqualTo((4 + RESERVED_BYTES) * 4L);

        assertThat(getInt(pageA, 0)).isEqualTo(2);
        assertThat(getInt(pageB, 0)).isEqualTo(3);
        assertThat(getInt(pageC, 0)).isEqualTo(4);
        assertThat(getInt(pageD, 0)).isEqualTo(5);
    }

    @Test
    void positionedVectoredReadFromEmptyFileMustFillPagesWithZeros() throws Exception {
        assumeThat(RESERVED_BYTES).isEqualTo(0);
        Path file = file("file");
        PageSwapperFactory factory = createSwapperFactory(getFs());
        PageSwapper swapper = createSwapperAndFile(factory, file, 4);

        long page = createPage(4);
        putInt(page, 0, 1);
        assertThat(read(swapper, 0, new long[] {page}, new int[] {4 + RESERVED_BYTES}, 1))
                .isEqualTo(0L);
        assertThat(getInt(page, 0)).isEqualTo(0);
    }

    @Test
    void positionedVectoredReadBeyondEndOfFileMustFillPagesWithZeros() throws Exception {
        assumeThat(RESERVED_BYTES).isEqualTo(0);
        Path file = file("file");
        PageSwapperFactory factory = createSwapperFactory(getFs());
        PageSwapper swapper = createSwapperAndFile(factory, file, 4);

        long output = createPage(4);
        putInt(output, 0, 0xFFFF_FFFF);
        write(
                swapper,
                0,
                new long[] {output, output, output},
                new int[] {4 + RESERVED_BYTES, 4 + RESERVED_BYTES, 4 + RESERVED_BYTES},
                3,
                3);

        long pageA = createPage(4);
        long pageB = createPage(4);
        putInt(pageA, 0, -1);
        putInt(pageB, 0, -1);
        assertThat(read(swapper, 3, new long[] {pageA, pageB}, new int[] {4 + RESERVED_BYTES, 4 + RESERVED_BYTES}, 2))
                .isEqualTo(0L);
        assertThat(getInt(pageA, 0)).isEqualTo(0);
        assertThat(getInt(pageB, 0)).isEqualTo(0);
    }

    @Test
    void positionedVectoredReadBeyondEndOfFileMustFillPagesWithZerosForBuffersWithDifferentSizes() throws Exception {
        assumeThat(RESERVED_BYTES).isEqualTo(0);
        Path file = file("file");
        PageSwapperFactory factory = createSwapperFactory(getFs());
        PageSwapper swapper = createSwapperAndFile(factory, file, 4);

        long output = createPage(4);
        putInt(output, 0, 42);
        write(swapper, 0, new long[] {output, output, output, output}, new int[] {4, 4, 4, 4}, 4, 4);

        long pageA = createPage(4);
        long pageB = createPage(8);
        long pageC = createPage(12);
        putInt(pageA, 0, -1);
        putInt(pageB, 0, -1);
        putInt(pageB, 4, -1);
        putInt(pageC, 0, -1);
        putInt(pageC, 4, -1);
        putInt(pageC, 8, -1);
        assertThat(read(swapper, 0, new long[] {pageA, pageB, pageC}, new int[] {4, 8, 12}, 3))
                .isEqualTo(16L);
        assertThat(getInt(pageA, 0)).isEqualTo(42);
        assertThat(getInt(pageB, 0)).isEqualTo(42);
        assertThat(getInt(pageB, 4)).isEqualTo(42);
        assertThat(getInt(pageC, 0)).isEqualTo(42);
        assertThat(getInt(pageC, 4)).isEqualTo(0);
        assertThat(getInt(pageC, 8)).isEqualTo(0);
    }

    @Test
    void positionedVectoredReadWhereLastPageExtendBeyondEndOfFileMustHaveRemainderZeroFilled() throws Exception {
        assumeThat(RESERVED_BYTES).isEqualTo(0);
        Path file = file("file");
        PageSwapperFactory factory = createSwapperFactory(getFs());
        PageSwapper swapper = createSwapperAndFile(factory, file, 4);

        long output = createPage(4);
        putInt(output, 0, 0xFFFF_FFFF);
        write(swapper, 0, new long[] {output, output, output, output, output}, new int[] {4, 4, 4, 4, 4}, 5, 5);
        swapper.close();

        swapper = createSwapper(factory, file, 8, NO_CALLBACK, false, false);
        long pageA = createPage(8);
        long pageB = createPage(8);
        putLong(pageA, 0, X);
        putLong(pageB, 0, Y);
        assertThat(read(swapper, 1, new long[] {pageA, pageB}, new int[] {8, 8}, 2))
                .isIn(12L, 16L);
        assertThat(getLong(pageA, 0)).isEqualTo(0xFFFF_FFFF_FFFF_FFFFL);

        //        assertThat( getLong( 0, pageB ), is( 0xFFFF_FFFF_0000_0000L ) );
        assertThat(getByte(pageB, 0)).isEqualTo((byte) 0xFF);
        assertThat(getByte(pageB, 1)).isEqualTo((byte) 0xFF);
        assertThat(getByte(pageB, 2)).isEqualTo((byte) 0xFF);
        assertThat(getByte(pageB, 3)).isEqualTo((byte) 0xFF);
        assertThat(getByte(pageB, 4)).isEqualTo((byte) 0x00);
        assertThat(getByte(pageB, 5)).isEqualTo((byte) 0x00);
        assertThat(getByte(pageB, 6)).isEqualTo((byte) 0x00);
        assertThat(getByte(pageB, 7)).isEqualTo((byte) 0x00);
    }

    @Test
    void positionedVectoredReadWhereSecondLastPageExtendBeyondEndOfFileMustHaveRestZeroFilled() throws Exception {
        assumeThat(RESERVED_BYTES).isEqualTo(0);
        Path file = file("file");
        PageSwapperFactory factory = createSwapperFactory(getFs());
        PageSwapper swapper = createSwapperAndFile(factory, file, 4);

        long output = createPage(4);
        putInt(output, 0, 1);
        write(swapper, 0, output);
        putInt(output, 0, 2);
        write(swapper, 1, output);
        putInt(output, 0, 3);
        write(swapper, 2, output);
        swapper.close();

        swapper = createSwapper(factory, file, 8, NO_CALLBACK, false, false);
        long pageA = createPage(8);
        long pageB = createPage(8);
        long pageC = createPage(8);
        putInt(pageA, 0, -1);
        putInt(pageB, 0, -1);
        putInt(pageC, 0, -1);
        assertThat(read(swapper, 0, new long[] {pageA, pageB, pageC}, new int[] {8, 8, 8}, 3))
                .isIn(12L, 16L);
        assertThat(getInt(pageA, 0)).isEqualTo(1);
        assertThat(getInt(pageA, 4)).isEqualTo(2);
        assertThat(getInt(pageB, 0)).isEqualTo(3);
        assertThat(getInt(pageB, 4)).isEqualTo(0);
        assertThat(getLong(pageC, 0)).isEqualTo(0L);
    }

    @Test
    void concurrentPositionedVectoredReadsAndWritesMustNotInterfere() throws Exception {
        Path file = file("file");
        PageSwapperFactory factory = createSwapperFactory(getFs());
        final PageSwapper swapper = createSwapperAndFile(factory, file, 4);
        final int pageCount = 100;
        final int iterations = 20000;
        final CountDownLatch startLatch = new CountDownLatch(1);
        long output = createPage(4);
        for (int i = 0; i < pageCount; i++) {
            putInt(output, 0, i + 1);
            write(swapper, i, output);
        }

        Callable<Void> work = () -> {
            ThreadLocalRandom rng = ThreadLocalRandom.current();
            int length = 10;
            int pageSize = 4;
            long[] pages = new long[length];
            int[] sizes = new int[length];
            for (int i = 0; i < length; i++) {
                pages[i] = createPage(pageSize);
                sizes[i] = pageSize + RESERVED_BYTES;
            }

            startLatch.await();
            for (int i = 0; i < iterations; i++) {
                long startFilePageId = rng.nextLong(0, pageCount - pages.length);
                if (rng.nextBoolean()) {
                    // Do read
                    long bytesRead = read(swapper, startFilePageId, pages, sizes, pages.length);
                    assertThat(bytesRead).isEqualTo(pages.length * 4L + RESERVED_BYTES * length);
                    for (int j = 0; j < pages.length; j++) {
                        int expectedValue = (int) (1 + j + startFilePageId);
                        int actualValue = getInt(pages[j], 0);
                        assertThat(actualValue).isEqualTo(expectedValue);
                    }
                } else {
                    // Do write
                    for (int j = 0; j < pages.length; j++) {
                        int value = (int) (1 + j + startFilePageId);
                        putInt(pages[j], 0, value);
                    }
                    assertThat(write(swapper, startFilePageId, pages, sizes, pages.length, pages.length))
                            .isEqualTo(pages.length * 4L + RESERVED_BYTES * length);
                }
            }
            return null;
        };

        int threads = 8;
        ExecutorService executor = null;
        try {
            executor = Executors.newFixedThreadPool(threads, r -> {
                Thread thread = Executors.defaultThreadFactory().newThread(r);
                thread.setDaemon(true);
                return thread;
            });
            List<Future<?>> futures = new ArrayList<>(threads);
            for (int i = 0; i < threads; i++) {
                futures.add(executor.submit(work));
            }

            startLatch.countDown();
            Futures.getAll(futures);
        } finally {
            if (executor != null) {
                executor.shutdown();
            }
        }
    }

    @Test
    void mustThrowNullPointerExceptionFromReadWhenPageArrayIsNull() throws Exception {
        Path file = file("file");
        PageSwapperFactory factory = createSwapperFactory(getFs());
        PageSwapper swapper = createSwapperAndFile(factory, file, 4);

        long page = createPage(4);

        int[] pageSizes = {4 + RESERVED_BYTES, 4 + RESERVED_BYTES, 4 + RESERVED_BYTES, 4 + RESERVED_BYTES};
        write(swapper, 0, new long[] {page, page, page, page}, pageSizes, 4, 4);

        assertThatThrownBy(
                        () -> read(swapper, 0, null, pageSizes, 4), "vectored read with null array should have thrown")
                .extracting(ExceptionUtils::getRootCause)
                .isInstanceOf(NullPointerException.class);
    }

    @Test
    void mustThrowNullPointerExceptionFromWriteWhenPageArrayIsNull() throws Exception {
        Path file = file("file");
        PageSwapperFactory factory = createSwapperFactory(getFs());
        PageSwapper swapper = createSwapperAndFile(factory, file, 4);

        assertThatThrownBy(
                        () -> write(swapper, 0, null, null, 4, 4), "vectored write with null array should have thrown")
                .extracting(ExceptionUtils::getRootCause)
                .isInstanceOf(NullPointerException.class);
    }

    @Test
    void readMustThrowForNegativeFilePageIds() throws Exception {
        Path file = file("file");
        PageSwapperFactory factory = createSwapperFactory(getFs());
        PageSwapper swapper = createSwapperAndFile(factory, file, 4);

        assertThrows(IOException.class, () -> read(swapper, -1, createPage(4)));
    }

    @Test
    @DisabledOnOs(OS.LINUX)
    void directIOAllowedOnlyOnLinux() throws IOException {
        PageSwapperFactory factory = createSwapperFactory(getFs());
        Path file = file("file");
        var e = assertThrows(IllegalArgumentException.class, () -> createSwapperAndFile(factory, file, true));
        assertThat(e.getMessage()).contains("Linux");
    }

    @Test
    @EnabledOnOs(OS.LINUX)
    void doNotAllowDirectIOForPagesNotMultipleOfBlockSize() throws IOException {
        PageSwapperFactory factory = createSwapperFactory(getFs());
        Path file = file("file");
        checkUnsupportedPageSize(factory, file, 17);
        checkUnsupportedPageSize(factory, file, 115);
        checkUnsupportedPageSize(factory, file, 218);
        checkUnsupportedPageSize(factory, file, 419);
        checkUnsupportedPageSize(factory, file, 524);
        checkUnsupportedPageSize(factory, file, 1023);
        checkUnsupportedPageSize(factory, file, 4097);
    }

    private void checkUnsupportedPageSize(PageSwapperFactory factory, Path path, int pageSize) {
        var e = assertThrows(IllegalArgumentException.class, () -> createSwapperAndFile(factory, path, pageSize, true));
        assertThat(e.getMessage()).contains("block");
    }

    @Test
    void writeMustThrowForNegativeFilePageIds() throws Exception {
        Path file = file("file");
        PageSwapperFactory factory = createSwapperFactory(getFs());
        PageSwapper swapper = createSwapperAndFile(factory, file, 4);

        assertThrows(IOException.class, () -> write(swapper, -1, createPage(4)));
    }

    @Test
    void vectoredReadMustThrowForNegativeFilePageIds() throws Exception {
        Path file = file("file");
        PageSwapperFactory factory = createSwapperFactory(getFs());
        PageSwapper swapper = createSwapperAndFile(factory, file, 4);

        assertThrows(
                IOException.class,
                () -> read(
                        swapper,
                        -1,
                        new long[] {createPage(4), createPage(4)},
                        new int[] {4 + RESERVED_BYTES, 4 + RESERVED_BYTES},
                        2));
    }

    @Test
    void vectoredWriteMustThrowForNegativeFilePageIds() throws Exception {
        Path file = file("file");
        PageSwapperFactory factory = createSwapperFactory(getFs());
        PageSwapper swapper = createSwapperAndFile(factory, file, 4);

        assertThrows(
                IOException.class,
                () -> write(
                        swapper,
                        -1,
                        new long[] {createPage(4), createPage(4)},
                        new int[] {4 + RESERVED_BYTES, 4 + RESERVED_BYTES},
                        2,
                        2));
    }

    @Test
    void vectoredReadMustReadNothingWhenLengthIsZero() throws Exception {
        Path file = file("file");
        PageSwapperFactory factory = createSwapperFactory(getFs());
        PageSwapper swapper = createSwapperAndFile(factory, file, 4);

        long pageA = createPage(4);
        long pageB = createPage(4);
        putInt(pageA, 0, 1);
        putInt(pageB, 0, 2);
        long[] pages = {pageA, pageB};
        int[] pageSizes = {4 + RESERVED_BYTES, 4 + RESERVED_BYTES};
        write(swapper, 0, pages, pageSizes, 2, 2);
        putInt(pageA, 0, 3);
        putInt(pageB, 0, 4);
        read(swapper, 0, pages, pageSizes, 0);

        int[] expectedValues = {3, 4};
        int[] actualValues = {getInt(pageA, 0), getInt(pageB, 0)};
        assertThat(actualValues).isEqualTo(expectedValues);
    }

    @Test
    void vectoredWriteMustWriteNothingWhenLengthIsZero() throws Exception {
        Path file = file("file");
        PageSwapperFactory factory = createSwapperFactory(getFs());
        PageSwapper swapper = createSwapperAndFile(factory, file, 4);

        long pageA = createPage(4);
        long pageB = createPage(4);
        putInt(pageA, 0, 1);
        putInt(pageB, 0, 2);
        long[] pages = {pageA, pageB};
        int[] pageSizes = {4 + RESERVED_BYTES, 4 + RESERVED_BYTES};
        write(swapper, 0, pages, pageSizes, 2, 2);
        putInt(pageA, 0, 3);
        putInt(pageB, 0, 4);
        write(swapper, 0, pages, pageSizes, 0, 0);
        read(swapper, 0, pages, pageSizes, 2);

        int[] expectedValues = {1, 2};
        int[] actualValues = {getInt(pageA, 0), getInt(pageB, 0)};
        assertThat(actualValues).isEqualTo(expectedValues);
    }

    @Test
    void mustDeleteFileIfClosedWithCloseAndDelete() throws Exception {
        Path file = file("file");
        PageSwapperFactory factory = createSwapperFactory(getFs());
        PageSwapper swapper = createSwapperAndFile(factory, file, 4);
        swapper.closeAndDelete();

        assertThrows(
                IOException.class,
                () -> createSwapper(factory, file, 4, NO_CALLBACK, false, false),
                "should not have been able to create a page swapper for non-existing file");
    }

    protected final PageSwapperFactory createSwapperFactory(FileSystemAbstraction fileSystem) {
        return swapperFactory(fileSystem);
    }

    protected long createPage(int cachePageSize) {
        int size = cachePageSize + RESERVED_BYTES;
        long address = mman.allocateAligned(size + Integer.BYTES, 1);
        UnsafeUtil.putInt(address(address), size);
        return address(address) + Integer.BYTES;
    }

    protected static void clear(long address) {
        byte b = (byte) 0;
        for (int i = 0; i < PAYLOAD_SIZE; i++) {
            UnsafeUtil.putByte(address(address) + i, b);
        }
    }

    protected PageSwapper createSwapper(
            PageSwapperFactory factory,
            Path path,
            int filePageSize,
            PageEvictionCallback callback,
            boolean createIfNotExist)
            throws IOException {
        return createSwapper(factory, path, filePageSize, callback, createIfNotExist, false);
    }

    protected PageSwapper createSwapper(
            PageSwapperFactory factory,
            Path path,
            int filePageSize,
            PageEvictionCallback callback,
            boolean createIfNotExist,
            boolean useDirectIO)
            throws IOException {
        PageSwapper swapper = factory.createPageSwapper(
                path,
                filePageSize + RESERVED_BYTES,
                callback,
                createIfNotExist,
                useDirectIO,
                DISABLED,
                ALWAYS_ALLOW,
                swapperSet);
        openedSwappers.add(swapper);
        return swapper;
    }

    protected PageSwapper createSwapper(
            PageSwapperFactory factory,
            Path path,
            int filePageSize,
            PageEvictionCallback callback,
            boolean createIfNotExist,
            boolean useDirectIO,
            IOController controller)
            throws IOException {
        PageSwapper swapper = factory.createPageSwapper(
                path,
                filePageSize + RESERVED_BYTES,
                callback,
                createIfNotExist,
                useDirectIO,
                controller,
                ALWAYS_ALLOW,
                swapperSet);
        openedSwappers.add(swapper);
        return swapper;
    }

    protected static int sizeOfAsInt(long address) {
        return UnsafeUtil.getInt(address - Integer.BYTES) - RESERVED_BYTES;
    }

    protected static void putInt(long address, int offset, int value) {
        UnsafeUtil.putInt(address(address) + offset, value);
    }

    public static long address(long address) {
        return address + RESERVED_BYTES;
    }

    protected static int getInt(long address, int offset) {
        return UnsafeUtil.getInt(address(address) + offset);
    }

    protected static void putLong(long address, int offset, long value) {
        UnsafeUtil.putLong(address(address) + offset, value);
    }

    protected static long getLong(long address, int offset) {
        return UnsafeUtil.getLong(address(address) + offset);
    }

    protected static byte getByte(long address, int offset) {
        return UnsafeUtil.getByte(address(address) + offset);
    }

    private static long write(PageSwapper swapper, int filePageId, long address) throws IOException {
        return swapper.write(filePageId, address);
    }

    private static long read(PageSwapper swapper, int filePageId, long address) throws IOException {
        return swapper.read(filePageId, address);
    }

    private static long read(PageSwapper swapper, long startFilePageId, long[] pages, int[] pageSizes, int length)
            throws IOException {
        if (length == 0) {
            return 0;
        }
        return swapper.read(startFilePageId, pages, pageSizes, length);
    }

    private static long write(
            PageSwapper swapper, long startFilePageId, long[] pages, int[] pageSizes, int length, int affectedPages)
            throws IOException {
        if (length == 0) {
            return 0;
        }
        return swapper.write(startFilePageId, pages, pageSizes, length, affectedPages);
    }

    private static int cachePageSize() {
        return cachePageSize;
    }

    private long createPage() {
        return createPage(cachePageSize());
    }

    private PageSwapper createSwapperAndFile(PageSwapperFactory factory, Path path) throws IOException {
        return createSwapperAndFile(factory, path, PAYLOAD_SIZE);
    }

    private PageSwapper createSwapperAndFile(PageSwapperFactory factory, Path path, boolean useDirectIO)
            throws IOException {
        return createSwapper(factory, path, PAYLOAD_SIZE, NO_CALLBACK, true, useDirectIO);
    }

    private PageSwapper createSwapperAndFile(
            PageSwapperFactory factory, Path path, int filePageSize, boolean useDirectIO) throws IOException {
        return createSwapper(factory, path, filePageSize, NO_CALLBACK, true, useDirectIO);
    }

    private PageSwapper createSwapperAndFile(PageSwapperFactory factory, Path path, int filePageSize)
            throws IOException {
        return createSwapper(factory, path, filePageSize, NO_CALLBACK, true, false);
    }

    private Path file(String filename) throws IOException {
        Path file = testDir.file(filename);
        mkdirs(file.getParent());
        return file;
    }

    private static long sizeOfAsLong(long page) {
        return sizeOfAsInt(page);
    }
}
