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
package org.neo4j.io.pagecache.impl.muninn.swapper;

import static java.lang.Long.numberOfTrailingZeros;
import static java.util.Arrays.copyOfRange;
import static org.neo4j.io.pagecache.impl.muninn.StoreFile.segmentPath;
import static org.neo4j.util.Preconditions.requirePowerOfTwo;

import java.io.IOException;
import java.lang.invoke.ConstantBootstraps;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.nio.file.Path;
import java.util.Arrays;
import org.neo4j.internal.unsafe.UnsafeUtil;
import org.neo4j.io.async.AsyncBlockAccessor;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.pagecache.IOController;
import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.io.pagecache.PageEvictionCallback;
import org.neo4j.io.pagecache.impl.muninn.EvictionBouncer;
import org.neo4j.io.pagecache.impl.muninn.MuninnPageCache;
import org.neo4j.io.pagecache.tracing.PageCacheTracer;
import org.neo4j.io.pagecache.tracing.PageFileSwapperTracer;
import org.neo4j.io.pagecache.tracing.SegmentEvent;

public final class SegmentedPageSwapper implements PageSwapper {
    private static final VarHandle SEGMENTS = ConstantBootstraps.fieldVarHandle(
            MethodHandles.lookup(), "segments", VarHandle.class, SegmentedPageSwapper.class, PageSwapper[].class);
    private static final VarHandle SEGMENT_SLOT =
            ConstantBootstraps.arrayVarHandle(MethodHandles.lookup(), "_", VarHandle.class, PageSwapper[].class);

    private final Path basePath;
    private final int filePageSize;
    private final long pagesPerSegment;
    private final PageSwapperFactory fileSwapperFactory;
    private final FileSystemAbstraction fs;
    private final boolean useDirectIO;
    private final IOController ioController;
    private final EvictionBouncer evictionBouncer;
    private final int swapperId;
    private final PageFileSwapperTracer fileSwapperTracer;
    private final PageCacheTracer pageCacheTracer;
    private final int pageShift;
    private volatile PageEvictionCallback onEviction;

    private PageSwapper[] segments;

    SegmentedPageSwapper(
            Path basePath,
            int filePageSize,
            long pagesPerSegment,
            PageEvictionCallback onEviction,
            boolean createIfNotExist,
            boolean useDirectIO,
            IOController ioController,
            EvictionBouncer evictionBouncer,
            SwapperIdProvider swapperIdProvider,
            PageSwapperFactory fileSwapperFactory,
            FileSystemAbstraction fs,
            PageFileSwapperTracer fileSwapperTracer,
            PageCacheTracer pageCacheTracer)
            throws IOException {
        requirePowerOfTwo(pagesPerSegment);
        this.basePath = basePath;
        this.filePageSize = filePageSize;
        this.pagesPerSegment = pagesPerSegment;
        this.fs = fs;
        this.useDirectIO = useDirectIO;
        this.ioController = ioController;
        this.evictionBouncer = evictionBouncer;
        this.fileSwapperTracer = fileSwapperTracer;
        this.pageCacheTracer = pageCacheTracer;
        this.onEviction = onEviction;
        this.fileSwapperFactory = fileSwapperFactory;
        this.pageShift = numberOfTrailingZeros(pagesPerSegment);

        this.swapperId = swapperIdProvider.swapperId(this);
        this.segments = initialSegmentOpen(createIfNotExist);
    }

    @Override
    public long read(long filePageId, long bufferAddress) throws IOException {
        PageSwapper segmentSwapper = segmentAt(segmentIndexFor(filePageId), true);
        if (segmentSwapper == null) {
            UnsafeUtil.setMemory(bufferAddress, filePageSize, MuninnPageCache.ZERO_BYTE);
            return 0;
        }
        return segmentSwapper.read(pageWithinSegment(filePageId), bufferAddress);
    }

    @Override
    public long read(long startFilePageId, long[] bufferAddresses, int[] bufferLengths, int length) throws IOException {
        long totalBytes = 0;

        long pageId = startFilePageId;
        int segment = segmentIndexFor(pageId);
        long bytesLeft = bytesLeftInSegment(pageId);
        long bytesToRead = 0;
        int buffersToRead = 0;
        for (int i = 0; i < length; i++) {
            buffersToRead++;
            if (bytesToRead + bufferLengths[i] < bytesLeft) {
                bytesToRead += bufferLengths[i];
            } else {
                int originalLength = bufferLengths[buffersToRead - 1];
                bufferLengths[buffersToRead - 1] = Math.toIntExact(bytesLeft - bytesToRead);

                PageSwapper pageSwapper = segmentAt(segment, true);
                if (pageSwapper == null) {
                    clearBuffers(bufferAddresses, bufferLengths, buffersToRead);
                    return totalBytes;
                }
                totalBytes +=
                        pageSwapper.read(pageWithinSegment(pageId), bufferAddresses, bufferLengths, buffersToRead);

                // patching buffer addresses and forcing loop to check current buffer again if we did the partial read
                int bufferLength = bufferLengths[buffersToRead - 1];
                if (originalLength != bufferLength) {
                    bufferLengths[buffersToRead - 1] = Math.toIntExact(originalLength - bufferLength);
                    bufferAddresses[buffersToRead - 1] = bufferAddresses[buffersToRead - 1] + bufferLength;
                    buffersToRead -= 1;
                }

                segment++;
                pageId = logicalPageOf(segment, 0);
                bytesLeft = bytesLeftInSegment(pageId);
                // TODO: add offsets to generic methods?
                bufferAddresses = copyOfRange(bufferAddresses, buffersToRead, Math.min(length, bufferAddresses.length));
                bufferLengths = copyOfRange(bufferLengths, buffersToRead, Math.min(length, bufferLengths.length));
                length = bufferLengths.length;

                i = -1;
                buffersToRead = 0;
                bytesToRead = 0;
            }
        }
        if (bytesToRead > 0) {
            PageSwapper pageSwapper = segmentAt(segment, true);
            if (pageSwapper != null) {
                totalBytes +=
                        pageSwapper.read(pageWithinSegment(pageId), bufferAddresses, bufferLengths, buffersToRead);
            } else {
                clearBuffers(bufferAddresses, bufferLengths, buffersToRead);
                return totalBytes;
            }
        }
        return totalBytes;
    }

    private static void clearBuffers(long[] bufferAddresses, int[] bufferLengths, int buffersToRead) {
        for (int j = 0; j < buffersToRead; j++) {
            UnsafeUtil.setMemory(bufferAddresses[j], bufferLengths[j], MuninnPageCache.ZERO_BYTE);
        }
    }

    @Override
    public long write(long filePageId, long bufferAddress) throws IOException {
        return segmentAt(segmentIndexFor(filePageId), false).write(pageWithinSegment(filePageId), bufferAddress);
    }

    @Override
    public long write(long filePageId, long bufferAddress, int bufferLength) throws IOException {
        if (filePageId < 0) {
            throw new IOException("Invalid page id: " + filePageId);
        }

        long totalWrite = 0;
        long bytesToWrite = bufferLength;
        long writePageId = filePageId;
        long writeAddress = bufferAddress;

        while (bytesToWrite > 0) {
            int segmentIndex = segmentIndexFor(writePageId);
            long pagesLeftInSegment = pagesLeftInSegment(writePageId);
            int writeLength = Math.toIntExact(Math.min(pagesLeftInSegment * filePageSize, bytesToWrite));
            long writeBytes =
                    segmentAt(segmentIndex, false).write(pageWithinSegment(writePageId), writeAddress, writeLength);

            totalWrite += writeBytes;
            bytesToWrite -= writeBytes;
            writeAddress += writeBytes;
            writePageId += pagesLeftInSegment;
        }
        return totalWrite;
    }

    @Override
    public long write(long startFilePageId, long[] bufferAddresses, int[] bufferLengths, int length)
            throws IOException {
        long totalBytes = 0;

        long pageId = startFilePageId;
        int segment = segmentIndexFor(pageId);
        long bytesLeft = bytesLeftInSegment(pageId);
        long bytesToWrite = 0;
        int buffersToWrite = 0;
        for (int i = 0; i < length; i++) {
            buffersToWrite++;
            if (bytesToWrite + bufferLengths[i] < bytesLeft) {
                bytesToWrite += bufferLengths[i];
            } else {
                int originalLength = bufferLengths[buffersToWrite - 1];
                bufferLengths[buffersToWrite - 1] = Math.toIntExact(bytesLeft - bytesToWrite);

                totalBytes += segmentAt(segment, false)
                        .write(pageWithinSegment(pageId), bufferAddresses, bufferLengths, buffersToWrite);

                // patching buffer addresses and forcing loop to check current buffer again if we did the partial write
                int bufferLength = bufferLengths[buffersToWrite - 1];
                if (originalLength != bufferLength) {
                    bufferLengths[buffersToWrite - 1] = Math.toIntExact(originalLength - bufferLength);
                    bufferAddresses[buffersToWrite - 1] = bufferAddresses[buffersToWrite - 1] + bufferLength;
                    buffersToWrite -= 1;
                }

                segment++;
                pageId = logicalPageOf(segment, 0);
                bytesLeft = bytesLeftInSegment(pageId);
                // TODO: add offsets to generic methods?
                bufferAddresses =
                        copyOfRange(bufferAddresses, buffersToWrite, Math.min(length, bufferAddresses.length));
                bufferLengths = copyOfRange(bufferLengths, buffersToWrite, Math.min(length, bufferLengths.length));
                length = bufferLengths.length;

                i = -1;
                buffersToWrite = 0;
                bytesToWrite = 0;
            }
        }
        if (bytesToWrite > 0) {
            totalBytes += segmentAt(segment, false)
                    .write(pageWithinSegment(pageId), bufferAddresses, bufferLengths, buffersToWrite);
        }
        return totalBytes;
    }

    @Override
    public void asyncWrite(AsyncBlockAccessor accessor, long pageRef, long filePageId, long bufferAddress)
            throws IOException {
        segmentAt(segmentIndexFor(filePageId), false)
                .asyncWrite(accessor, pageRef, pageWithinSegment(filePageId), bufferAddress);
    }

    @Override
    public void asyncWrite(
            AsyncBlockAccessor accessor,
            long startFilePageId,
            long[] bufferAddresses,
            int[] bufferLengths,
            int length,
            long[] pageRefs,
            long[] flushStamps,
            int pagesToFlush)
            throws IOException {
        long pageId = startFilePageId;
        int segment = segmentIndexFor(pageId);
        long bytesLeft = bytesLeftInSegment(pageId);
        long bytesToWrite = 0;
        int buffersToWrite = 0;
        for (int i = 0; i < length; i++) {
            buffersToWrite++;
            if (bytesToWrite + bufferLengths[i] < bytesLeft) {
                bytesToWrite += bufferLengths[i];
            } else {
                int originalLength = bufferLengths[buffersToWrite - 1];
                bufferLengths[buffersToWrite - 1] = Math.toIntExact(bytesLeft - bytesToWrite);

                // Crossing a segment boundary fills the rest of the current segment exactly.
                int segmentPages = Math.toIntExact(bytesLeft / filePageSize);
                segmentAt(segment, false)
                        .asyncWrite(
                                accessor,
                                pageWithinSegment(pageId),
                                bufferAddresses,
                                bufferLengths,
                                buffersToWrite,
                                pageRefs,
                                flushStamps,
                                segmentPages);

                // patching buffer addresses and forcing loop to check current buffer again if we did the partial write
                int bufferLength = bufferLengths[buffersToWrite - 1];
                if (originalLength != bufferLength) {
                    bufferLengths[buffersToWrite - 1] = Math.toIntExact(originalLength - bufferLength);
                    bufferAddresses[buffersToWrite - 1] = bufferAddresses[buffersToWrite - 1] + bufferLength;
                    buffersToWrite -= 1;
                }

                segment++;
                pageId = logicalPageOf(segment, 0);
                bytesLeft = bytesLeftInSegment(pageId);
                bufferAddresses =
                        copyOfRange(bufferAddresses, buffersToWrite, Math.min(length, bufferAddresses.length));
                bufferLengths = copyOfRange(bufferLengths, buffersToWrite, Math.min(length, bufferLengths.length));
                length = bufferLengths.length;
                pageRefs = copyOfRange(pageRefs, segmentPages, Math.min(pagesToFlush, pageRefs.length));
                flushStamps = copyOfRange(flushStamps, segmentPages, Math.min(pagesToFlush, flushStamps.length));
                pagesToFlush -= segmentPages;

                i = -1;
                buffersToWrite = 0;
                bytesToWrite = 0;
            }
        }
        if (bytesToWrite > 0) {
            segmentAt(segment, false)
                    .asyncWrite(
                            accessor,
                            pageWithinSegment(pageId),
                            bufferAddresses,
                            bufferLengths,
                            buffersToWrite,
                            pageRefs,
                            flushStamps,
                            pagesToFlush);
        }
    }

    @Override
    public void evicted(long pageRef, long pageId) {
        PageEvictionCallback callback = this.onEviction;
        if (callback != null) {
            callback.onEvict(pageRef, pageId);
        }
    }

    @Override
    public Path path() {
        return basePath;
    }

    @Override
    public synchronized void close() throws IOException {
        IOException closeException = null;
        PageSwapper[] current = (PageSwapper[]) SEGMENTS.getAcquire(this);
        for (int i = 0; i < current.length; i++) {
            PageSwapper segmentSwapper = current[i];
            if (segmentSwapper == null) {
                continue;
            }
            try (SegmentEvent unloadEvent = pageCacheTracer.unloadSegment(basePath, i)) {
                segmentSwapper.close();
            } catch (IOException e) {
                if (closeException == null) {
                    closeException = e;
                } else {
                    closeException.addSuppressed(e);
                }
            }
        }
        if (closeException != null) {
            throw closeException;
        }
        onEviction = null;
    }

    @Override
    public synchronized void closeAndDelete() throws IOException {
        closeAndDeleteSegments((PageSwapper[]) SEGMENTS.getAcquire(this), 0);
        onEviction = null;
    }

    @Override
    public void force() throws IOException {
        PageSwapper[] current = (PageSwapper[]) SEGMENTS.getAcquire(this);
        for (PageSwapper segment : current) {
            if (segment != null) {
                segment.force();
            }
        }
    }

    @Override
    public long getLastPageId() throws IOException {
        PageSwapper[] swappers = (PageSwapper[]) SEGMENTS.getAcquire(this);
        if (swappers.length == 0) {
            return PageCursor.UNBOUND_PAGE_ID;
        }
        for (int lastSegment = swappers.length - 1; lastSegment >= 0; lastSegment--) {
            PageSwapper segment = openOrGrow(lastSegment, true);
            if (segment != null) {
                long lastPageId = segment.getLastPageId();
                if (lastPageId == PageCursor.UNBOUND_PAGE_ID) {
                    continue;
                }
                return lastSegment * pagesPerSegment + lastPageId;
            }
        }
        return PageCursor.UNBOUND_PAGE_ID;
    }

    @Override
    public void truncate() throws IOException {
        PageSwapper[] segments = (PageSwapper[]) SEGMENTS.getAcquire(this);
        if (segments.length == 0) {
            return;
        }
        segments[0].truncate();
        if (segments.length == 1) {
            return;
        }
        closeAndDeleteSegments(segments, 1);
        SEGMENTS.setRelease(this, new PageSwapper[] {segments[0]});
    }

    @Override
    public synchronized void truncate(long size) throws IOException {
        long pagesToKeep = size / filePageSize;
        PageSwapper[] current = (PageSwapper[]) SEGMENTS.getAcquire(this);
        if (current.length == 0) {
            return;
        }
        int lastKeptSegment = pagesToKeep == 0 ? 0 : segmentIndexFor(pagesToKeep - 1);
        if (lastKeptSegment >= current.length) {
            return;
        }
        long pagesInTheLastSegment = pagesToKeep - logicalPageOf(lastKeptSegment, 0);
        current[lastKeptSegment].truncate(pagesInTheLastSegment * filePageSize);

        closeAndDeleteSegments(current, lastKeptSegment + 1);
        SEGMENTS.setRelease(this, Arrays.copyOf(current, lastKeptSegment + 1));
    }

    @Override
    public boolean canAllocate() {
        PageSwapper[] current = (PageSwapper[]) SEGMENTS.getAcquire(this);
        return current[0].canAllocate();
    }

    @Override
    public void allocate(long newFileSize) throws IOException {
        long maxSegmentBytes = pagesPerSegment * (long) filePageSize;
        int fullSegments = Math.toIntExact(newFileSize / maxSegmentBytes);
        long remainderBytes = newFileSize % maxSegmentBytes;
        for (int segment = 0; segment < fullSegments; segment++) {
            segmentAt(segment, false).allocate(maxSegmentBytes);
        }
        if (remainderBytes > 0) {
            segmentAt(fullSegments, false).allocate(remainderBytes);
        }
    }

    @Override
    public int swapperId() {
        return swapperId;
    }

    @Override
    public PageFileSwapperTracer fileSwapperTracer() {
        return fileSwapperTracer;
    }

    @Override
    public boolean isPageFlushable(long pageRef) {
        return evictionBouncer.allowPageFlush(pageRef);
    }

    @Override
    public String toString() {
        PageSwapper[] current = (PageSwapper[]) SEGMENTS.getAcquire(this);
        return "SegmentedPageSwapper{filePageSize=" + filePageSize + ", pagesPerSegment=" + pagesPerSegment
                + ", segments=" + current.length + ", file=" + basePath + '}';
    }

    int segmentIndexFor(long pageId) {
        return (int) (pageId >>> pageShift);
    }

    long pageWithinSegment(long logicalPageId) {
        return (logicalPageId & (pagesPerSegment - 1));
    }

    long bytesLeftInSegment(long logicalPageId) {
        return pagesLeftInSegment(logicalPageId) * filePageSize;
    }

    long pagesLeftInSegment(long logicalPageId) {
        return (pagesPerSegment - pageWithinSegment(logicalPageId));
    }

    private long logicalPageOf(int segmentIndex, long pageWithinSegment) {
        return ((long) segmentIndex << pageShift) + pageWithinSegment;
    }

    private PageSwapper segmentAt(int index, boolean readOnly) throws IOException {
        if (index < 0) {
            throw new IOException("Incorrect segment index: " + index);
        }
        PageSwapper[] segments = (PageSwapper[]) SEGMENTS.getAcquire(this);
        if (index < segments.length) {
            PageSwapper segment = segments[index];
            if (segment != null) {
                return segment;
            }
        }
        return openOrGrow(index, readOnly);
    }

    private synchronized PageSwapper openOrGrow(int targetIndex, boolean readOnly) throws IOException {
        PageSwapper[] segments = (PageSwapper[]) SEGMENTS.getAcquire(this);
        if (targetIndex < segments.length) {
            PageSwapper existing = (PageSwapper) SEGMENT_SLOT.getAcquire(segments, targetIndex);
            if (existing != null) {
                return existing;
            }
            try (SegmentEvent loadEvent = pageCacheTracer.loadSegment(basePath, targetIndex)) {
                PageSwapper opened = openSegment(segmentPath(basePath, targetIndex), false);
                SEGMENT_SLOT.setRelease(segments, targetIndex, opened);
                return opened;
            }
        }
        if (readOnly) {
            return null;
        }
        return grow(segments, targetIndex);
    }

    private PageSwapper grow(PageSwapper[] pageSwappers, int targetIndex) throws IOException {
        int oldLength = pageSwappers.length;
        PageSwapper[] grown = Arrays.copyOf(pageSwappers, targetIndex + 1);
        for (int idx = oldLength; idx <= targetIndex; idx++) {
            try (SegmentEvent createEvent = pageCacheTracer.createSegment(basePath, idx)) {
                grown[idx] = openSegment(segmentPath(basePath, idx), true);
            }
        }
        SEGMENTS.setRelease(this, grown);
        return grown[targetIndex];
    }

    private PageSwapper[] initialSegmentOpen(boolean createIfNotExist) throws IOException {
        boolean preExisting = fs.fileExists(basePath);
        try (SegmentEvent event =
                preExisting ? pageCacheTracer.loadSegment(basePath, 0) : pageCacheTracer.createSegment(basePath, 0)) {
            PageSwapper segmentZero = openSegment(basePath, createIfNotExist);
            int existingCount = 1;
            while (fs.fileExists(segmentPath(basePath, existingCount))) {
                existingCount++;
            }
            PageSwapper[] segments = new PageSwapper[existingCount];
            segments[0] = segmentZero;
            return segments;
        }
    }

    private PageSwapper openSegment(Path path, boolean createIfNotExist) throws IOException {
        return fileSwapperFactory.createPageSwapper(
                path,
                filePageSize,
                onEviction,
                createIfNotExist,
                useDirectIO,
                pagesPerSegment,
                ioController,
                evictionBouncer,
                segmentSwapper -> swapperId);
    }

    private void closeAndDeleteSegments(PageSwapper[] segments, int initialIndex) throws IOException {
        IOException closeAndDeleteException = null;
        for (int i = initialIndex; i < segments.length; i++) {
            PageSwapper segment = segments[i];
            try (SegmentEvent deleteEvent = pageCacheTracer.deleteSegment(basePath, i)) {
                if (segment != null) {
                    segment.closeAndDelete();
                } else {
                    deleteSegmentFileIfExists(i);
                }
            } catch (IOException e) {
                if (closeAndDeleteException == null) {
                    closeAndDeleteException = e;
                } else {
                    closeAndDeleteException.addSuppressed(e);
                }
            }
        }
        if (closeAndDeleteException != null) {
            throw closeAndDeleteException;
        }
    }

    private void deleteSegmentFileIfExists(int segmentIndex) throws IOException {
        fs.deleteFile(segmentPath(basePath, segmentIndex));
    }
}
