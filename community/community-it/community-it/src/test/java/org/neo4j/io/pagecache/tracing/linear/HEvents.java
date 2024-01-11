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
package org.neo4j.io.pagecache.tracing.linear;

import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.io.StringReader;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.IdentityHashMap;
import java.util.Map;
import org.neo4j.io.pagecache.PageSwapper;
import org.neo4j.io.pagecache.tracing.EvictionEvent;
import org.neo4j.io.pagecache.tracing.EvictionRunEvent;
import org.neo4j.io.pagecache.tracing.FileFlushEvent;
import org.neo4j.io.pagecache.tracing.FlushEvent;
import org.neo4j.io.pagecache.tracing.PageReferenceTranslator;
import org.neo4j.io.pagecache.tracing.PinEvent;
import org.neo4j.io.pagecache.tracing.PinPageFaultEvent;

/**
 * Container of events for page cache tracers that are used to build linear historical representation of page cache
 * events.
 * In case if event can generate any other event it will properly add it to corresponding tracer and it will be also
 * tracked.
 * @see LinearHistoryTracer
 */
class HEvents {
    private HEvents() {}

    static final class EndHEvent extends HEvent {
        private static final Map<Class<?>, String> classSimpleNameCache = new IdentityHashMap<>();
        IntervalHEvent event;

        EndHEvent(IntervalHEvent event) {
            this.event = event;
        }

        @Override
        public void print(PrintStream out, String exceptionLinePrefix) {
            out.print('-');
            super.print(out, exceptionLinePrefix);
        }

        @Override
        void printBody(PrintStream out, String exceptionLinePrefix) {
            out.print(", elapsedMicros:");
            out.print((time - event.time) / 1000);
            out.print(", endOf:");
            Class<? extends IntervalHEvent> eventClass = event.getClass();
            String className = classSimpleNameCache.computeIfAbsent(eventClass, k -> eventClass.getSimpleName());
            out.print(className);
            out.print('#');
            out.print(System.identityHashCode(event));
        }
    }

    static class MappedFileHEvent extends HEvent {
        Path path;

        MappedFileHEvent(Path path) {
            this.path = path;
        }

        @Override
        void printBody(PrintStream out, String exceptionLinePrefix) {
            print(out, path);
        }
    }

    static class UnmappedFileHEvent extends HEvent {
        Path path;

        UnmappedFileHEvent(Path path) {
            this.path = path;
        }

        @Override
        void printBody(PrintStream out, String exceptionLinePrefix) {
            print(out, path);
        }
    }

    public static class EvictionRunHEvent extends IntervalHEvent implements EvictionRunEvent {
        int pagesToEvict;

        EvictionRunHEvent(LinearHistoryTracer tracer, int pagesToEvict) {
            super(tracer);
            this.pagesToEvict = pagesToEvict;
        }

        @Override
        public EvictionEvent beginEviction(long cachePageId) {
            return tracer.add(new EvictionHEvent(tracer, cachePageId));
        }

        @Override
        void printBody(PrintStream out, String exceptionLinePrefix) {
            out.print(", pagesToEvict:");
            out.print(pagesToEvict);
        }

        @Override
        public void freeListSize(int size) {
            // empty
        }
    }

    public static class FlushHEvent extends IntervalHEvent implements FlushEvent {
        private final long[] pageRefs;
        private final int pagesToFlush;
        private int pageMerged;
        private int pageCount;
        private final Path path;
        private int bytesWritten;
        private IOException exception;

        FlushHEvent(
                LinearHistoryTracer tracer, long[] pageRefs, PageSwapper swapper, int pagesToFlush, int pageMerged) {
            super(tracer);
            this.pageRefs = pageRefs;
            this.pagesToFlush = pagesToFlush;
            this.pageMerged = pageMerged;
            this.pageCount = 1;
            this.path = swapper.path();
        }

        @Override
        public void addBytesWritten(long bytes) {
            bytesWritten += bytes;
        }

        @Override
        public void close() {
            super.close();
        }

        @Override
        public void setException(IOException exception) {
            this.exception = exception;
        }

        @Override
        public void addPagesFlushed(int pageCount) {
            this.pageCount = pageCount;
        }

        @Override
        public void addEvictionFlushedPages(int pageCount) {
            addPagesFlushed(pageCount);
        }

        @Override
        public void addPagesMerged(int pagesMerged) {
            this.pageMerged = pagesMerged;
        }

        @Override
        void printBody(PrintStream out, String exceptionLinePrefix) {
            out.print(", pageRefs:");
            out.print(Arrays.toString(pageRefs));
            out.print(", pageCount:");
            out.print(pageCount);
            print(out, path);
            out.print(", bytesWritten:");
            out.print(bytesWritten);
            out.print(", pagesToFlush:");
            out.print(pagesToFlush);
            out.print(", PagesMerged:");
            out.print(pageMerged);
            print(out, exception, exceptionLinePrefix);
        }
    }

    public static class FileFlushHEvent extends IntervalHEvent implements FileFlushEvent {
        private final Path path;

        FileFlushHEvent(LinearHistoryTracer tracer, Path path) {
            super(tracer);
            this.path = path;
        }

        @Override
        public FlushEvent beginFlush(
                long[] pageRefs,
                PageSwapper swapper,
                PageReferenceTranslator pageReferenceTranslator,
                int pagesToFlush,
                int mergedPages) {
            return tracer.add(new FlushHEvent(tracer, pageRefs, swapper, pagesToFlush, mergedPages));
        }

        @Override
        public FlushEvent beginFlush(
                long pageRef, PageSwapper swapper, PageReferenceTranslator pageReferenceTranslator) {
            return tracer.add(new FlushHEvent(tracer, new long[] {pageRef}, swapper, 1, 0));
        }

        @Override
        public void startFlush(int[][] translationTable) {}

        @Override
        public void reset() {}

        @Override
        public long ioPerformed() {
            return 0;
        }

        @Override
        public long limitedNumberOfTimes() {
            return 0;
        }

        @Override
        public long limitedMillis() {
            return 0;
        }

        @Override
        public long pagesFlushed() {
            return 0;
        }

        @Override
        public ChunkEvent startChunk(int[] chunk) {
            return ChunkEvent.NULL;
        }

        @Override
        public void throttle(long recentlyCompletedIOs, long millis) {}

        @Override
        public void reportIO(int completedIOs) {}

        @Override
        public long localBytesWritten() {
            return 0;
        }

        @Override
        void printBody(PrintStream out, String exceptionLinePrefix) {
            print(out, path);
        }
    }

    public static class PinHEvent extends IntervalHEvent implements PinEvent {
        private final boolean exclusiveLock;
        private final long filePageId;
        private final Path path;
        private long cachePageId;
        private boolean hit;

        PinHEvent(LinearHistoryTracer tracer, boolean exclusiveLock, long filePageId, PageSwapper swapper) {
            super(tracer);
            this.exclusiveLock = exclusiveLock;
            this.filePageId = filePageId;
            this.hit = true;
            this.path = swapper.path();
        }

        @Override
        public void setCachePageId(long cachePageId) {
            this.cachePageId = cachePageId;
        }

        @Override
        public PinPageFaultEvent beginPageFault(long filePageId, PageSwapper swapper) {
            hit = false;
            return tracer.add(new PinPageFaultHEvent(tracer));
        }

        @Override
        public void noFault() {}

        @Override
        public void snapshotsLoaded(int oldSnapshotsLoaded) {}

        @Override
        public void hit() {}

        @Override
        void printBody(PrintStream out, String exceptionLinePrefix) {
            out.print(", filePageId:");
            out.print(filePageId);
            out.print(", cachePageId:");
            out.print(cachePageId);
            out.print(", hit:");
            out.print(hit);
            print(out, path);
            out.append(", exclusiveLock:");
            out.print(exclusiveLock);
        }
    }

    public static class PinPageFaultHEvent extends IntervalHEvent implements PinPageFaultEvent {
        private int bytesRead;
        private long cachePageId;
        private boolean pageEvictedByFaulter;
        private Throwable exception;

        PinPageFaultHEvent(LinearHistoryTracer linearHistoryTracer) {
            super(linearHistoryTracer);
        }

        @Override
        public void addBytesRead(long bytes) {
            bytesRead += bytes;
        }

        @Override
        public void setCachePageId(long cachePageId) {
            this.cachePageId = cachePageId;
        }

        @Override
        public void close() {
            super.close();
        }

        @Override
        public void setException(Throwable throwable) {
            this.exception = throwable;
        }

        @Override
        public void freeListSize(int freeListSize) {}

        @Override
        public EvictionEvent beginEviction(long cachePageId) {
            pageEvictedByFaulter = true;
            return tracer.add(new EvictionHEvent(tracer, cachePageId));
        }

        @Override
        void printBody(PrintStream out, String exceptionLinePrefix) {
            out.print(", cachePageId:");
            out.print(cachePageId);
            out.print(", bytesRead:");
            out.print(bytesRead);
            out.print(", pageEvictedByFaulter:");
            out.print(pageEvictedByFaulter);
            print(out, exception, exceptionLinePrefix);
        }
    }

    public static class EvictionHEvent extends IntervalHEvent implements EvictionEvent {
        private long filePageId;
        private Path path;
        private IOException exception;
        private final long cachePageId;

        EvictionHEvent(LinearHistoryTracer linearHistoryTracer, long cachePageId) {
            super(linearHistoryTracer);
            this.cachePageId = cachePageId;
        }

        @Override
        public void setFilePageId(long filePageId) {
            this.filePageId = filePageId;
        }

        @Override
        public void setSwapper(PageSwapper swapper) {
            path = swapper == null ? null : swapper.path();
        }

        @Override
        public void setException(IOException exception) {
            this.exception = exception;
        }

        @Override
        public FlushEvent beginFlush(
                long pageRef, PageSwapper swapper, PageReferenceTranslator pageReferenceTranslator) {
            return tracer.add(new FlushHEvent(tracer, new long[] {pageRef}, swapper, 1, 0));
        }

        @Override
        void printBody(PrintStream out, String exceptionLinePrefix) {
            out.print(", filePageId:");
            out.print(filePageId);
            out.print(", cachePageId:");
            out.print(cachePageId);
            print(out, path);
            print(out, exception, exceptionLinePrefix);
        }
    }

    public abstract static class HEvent {
        static final HEvent end = new HEvent() {
            @Override
            void printBody(PrintStream out, String exceptionLinePrefix) {
                out.print(" EOF ");
            }
        };

        final long time;
        final long threadId;
        final String threadName;
        volatile HEvent prev;

        HEvent() {
            time = System.nanoTime();
            Thread thread = Thread.currentThread();
            threadId = thread.getId();
            threadName = thread.getName();
            System.identityHashCode(this);
        }

        public static HEvent reverse(HEvent events) {
            HEvent current = end;
            while (events != end) {
                HEvent prev;
                do {
                    prev = events.prev;
                } while (prev == null);
                events.prev = current;
                current = events;
                events = prev;
            }
            return current;
        }

        public void print(PrintStream out, String exceptionLinePrefix) {
            out.print(getClass().getSimpleName());
            out.print('#');
            out.print(System.identityHashCode(this));
            out.print('[');
            out.print("time:");
            out.print((time - end.time) / 1000);
            out.print(", threadId:");
            out.print(threadId);
            printBody(out, exceptionLinePrefix);
            out.print(']');
        }

        abstract void printBody(PrintStream out, String exceptionLinePrefix);

        protected static void print(PrintStream out, Path file) {
            out.print(", file:");
            out.print(file == null ? "<null>" : file);
        }

        protected static void print(PrintStream out, Throwable exception, String linePrefix) {
            if (exception != null) {
                out.println(", exception:");
                ByteArrayOutputStream buf = new ByteArrayOutputStream();
                PrintStream sbuf = new PrintStream(buf);
                exception.printStackTrace(sbuf);
                sbuf.flush();
                BufferedReader reader = new BufferedReader(new StringReader(buf.toString()));
                try {
                    String line = reader.readLine();
                    while (line != null) {
                        out.print(linePrefix);
                        out.print('\t');
                        out.println(line);
                        line = reader.readLine();
                    }
                    out.print(linePrefix);
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
        }
    }

    public abstract static class IntervalHEvent extends HEvent {
        protected LinearHistoryTracer tracer;

        IntervalHEvent(LinearHistoryTracer tracer) {
            this.tracer = tracer;
        }

        public void close() {
            tracer.add(new EndHEvent(this));
        }
    }
}
