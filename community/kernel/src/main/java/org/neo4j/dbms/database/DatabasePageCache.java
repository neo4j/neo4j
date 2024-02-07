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
package org.neo4j.dbms.database;

import static java.util.Objects.requireNonNull;
import static org.neo4j.configuration.GraphDatabaseInternalSettings.snapshot_query;
import static org.neo4j.dbms.database.TicketMachine.Barrier.NO_BARRIER;
import static org.neo4j.io.pagecache.PageCacheOpenOptions.CONTEXT_VERSION_UPDATES;

import java.io.IOException;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import org.eclipse.collections.api.set.ImmutableSet;
import org.neo4j.configuration.Config;
import org.neo4j.dbms.database.TicketMachine.Barrier;
import org.neo4j.dbms.database.TicketMachine.Ticket;
import org.neo4j.io.pagecache.IOController;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.io.pagecache.PagedFile;
import org.neo4j.io.pagecache.buffer.IOBufferFactory;
import org.neo4j.io.pagecache.context.CursorContext;
import org.neo4j.io.pagecache.impl.muninn.EvictionBouncer;
import org.neo4j.io.pagecache.impl.muninn.VersionStorage;
import org.neo4j.io.pagecache.monitoring.PageFileCounters;
import org.neo4j.io.pagecache.tracing.DatabaseFlushEvent;
import org.neo4j.io.pagecache.tracing.FileFlushEvent;
import org.neo4j.io.pagecache.tracing.FileMappedListener;
import org.neo4j.io.pagecache.tracing.version.FileTruncateEvent;

/**
 * Wrapper around global page cache for an individual database. Abstracts the knowledge that database can have about other databases mapped files
 * by restricting access to files that mapped by other databases.
 * Any lookup or attempts to flush/close page file or cache itself will influence only files that were mapped by particular database over this wrapper.
 * Database specific page cache lifecycle tight to an individual database, and it will be closed as soon as the particular database will be closed.
 */
public class DatabasePageCache implements PageCache {

    private final PageCache globalPageCache;
    private final Map<Path, DatabasePagedFile> uniqueDatabasePagedFiles = new ConcurrentHashMap<>();
    private final IOController ioController;
    private final List<FileMappedListener> mappedListeners = new CopyOnWriteArrayList<>();
    private final boolean useSnapshotEngine;
    private boolean closed;
    private final TicketMachine ticketMachine = new TicketMachine();
    private final VersionStorage versionStorage;

    public DatabasePageCache(
            PageCache globalPageCache, IOController ioController, VersionStorage versionStorage, Config config) {
        this.globalPageCache = requireNonNull(globalPageCache);
        this.ioController = requireNonNull(ioController);
        this.versionStorage = requireNonNull(versionStorage);
        this.useSnapshotEngine = config.get(snapshot_query);
    }

    @Override
    public synchronized PagedFile map(
            Path path,
            int pageSize,
            String databaseName,
            ImmutableSet<OpenOption> openOptions,
            IOController ignoredController,
            EvictionBouncer evictionBouncer,
            VersionStorage ignoredVersionStorage)
            throws IOException {
        // no one should call this version of map method with emptyDatabaseName != null,
        // since it is this class that is decorating map calls with the name of the database
        if (useSnapshotEngine) {
            openOptions = openOptions.newWith(CONTEXT_VERSION_UPDATES);
        }
        PagedFile pagedFile = globalPageCache.map(
                path, pageSize, databaseName, openOptions, ioController, evictionBouncer, versionStorage);
        // Our default page cache handles mapping a file multiple times, where additional mappings for the
        // same file just returns the existing mapping. The DatabasePageCache needs to keep track of when
        // a file is mapped the first time _for this particular instance_ tho, so that listeners can be
        // invoked only when file is mapped first time and unmapped last time.
        var newMapping = new DatabasePagedFile(pagedFile, ticketMachine.newTicket());
        var existingMapping = uniqueDatabasePagedFiles.putIfAbsent(path, newMapping);
        if (existingMapping == null) {
            invokeFileMapListeners(mappedListeners, newMapping);
            return newMapping;
        } else {
            existingMapping.refCount.incrementAndGet();
            return existingMapping;
        }
    }

    @Override
    public Optional<PagedFile> getExistingMapping(Path path) {
        Path canonicalFile = path.normalize();
        return uniqueDatabasePagedFiles.values().stream()
                .filter(pagedFile -> pagedFile.path().equals(canonicalFile))
                .map(pf -> (PagedFile) pf)
                .findFirst();
    }

    @Override
    public List<PagedFile> listExistingMappings() {
        return new ArrayList<>(uniqueDatabasePagedFiles.values());
    }

    @Override
    public void flushAndForce(DatabaseFlushEvent flushEvent) throws IOException {
        flushAndForce(flushEvent, NO_BARRIER);
    }

    private void flushAndForce(DatabaseFlushEvent flushEvent, Barrier barrier) throws IOException {
        for (DatabasePagedFile pagedFile : uniqueDatabasePagedFiles.values()) {
            if (barrier.canPass(pagedFile.flushTicket())) {
                try (FileFlushEvent fileFlushEvent = flushEvent.beginFileFlush()) {
                    pagedFile.flushAndForce(fileFlushEvent);
                }
            }
        }
    }

    @Override
    public synchronized void close() {
        if (closed) {
            throw new IllegalStateException("Database page cache was already closed");
        }
        for (var pagedFile : uniqueDatabasePagedFiles.values()) {
            // We must call close an equal number of times that we this file have been mapped (and not yet unmapped)
            // pagedFile.close() will decrement refCount itself.
            while (pagedFile.refCount.get() > 0) {
                pagedFile.close();
            }
        }
        uniqueDatabasePagedFiles.clear();
        closed = true;
    }

    @Override
    public int pageSize() {
        return globalPageCache.pageSize();
    }

    @Override
    public int pageReservedBytes(ImmutableSet<OpenOption> openOptions) {
        return globalPageCache.pageReservedBytes(openOptions);
    }

    @Override
    public long maxCachedPages() {
        return globalPageCache.maxCachedPages();
    }

    @Override
    public long freePages() {
        return globalPageCache.freePages();
    }

    @Override
    public IOBufferFactory getBufferFactory() {
        return globalPageCache.getBufferFactory();
    }

    private static void invokeFileMapListeners(List<FileMappedListener> listeners, DatabasePagedFile databasePageFile) {
        for (FileMappedListener mappedListener : listeners) {
            mappedListener.fileMapped(databasePageFile);
        }
    }

    private static void invokeFileUnmapListeners(
            List<FileMappedListener> listeners, DatabasePagedFile databasePageFile) {
        for (FileMappedListener mappedListener : listeners) {
            mappedListener.fileUnmapped(databasePageFile);
        }
    }

    public void registerFileMappedListener(FileMappedListener mappedListener) {
        mappedListeners.add(mappedListener);
    }

    public void unregisterFileMappedListener(FileMappedListener mappedListener) {
        mappedListeners.remove(mappedListener);
    }

    public FlushGuard flushGuard(DatabaseFlushEvent flushEvent) {
        Barrier barrier = ticketMachine.nextBarrier();
        return () -> flushAndForce(flushEvent, barrier);
    }

    private synchronized void unmap(DatabasePagedFile databasePagedFile) {
        if (databasePagedFile.refCount.decrementAndGet() == 0) {
            invokeFileUnmapListeners(mappedListeners, databasePagedFile);
            uniqueDatabasePagedFiles.remove(databasePagedFile.path());
        }
    }

    /**
     * A flush guard to flush any mapped and un-flushed files since creation of the guard
     */
    @FunctionalInterface
    public interface FlushGuard {
        void flushUnflushed() throws IOException;
    }

    private class DatabasePagedFile implements PagedFile {
        private final PagedFile delegate;
        private final Ticket flushTicket;
        private final AtomicInteger refCount = new AtomicInteger(1);

        DatabasePagedFile(PagedFile delegate, Ticket flushTicket) {
            this.delegate = delegate;
            this.flushTicket = flushTicket;
        }

        @Override
        public PageCursor io(long pageId, int pf_flags, CursorContext context) throws IOException {
            return delegate.io(pageId, pf_flags, context);
        }

        @Override
        public int pageSize() {
            return delegate.pageSize();
        }

        @Override
        public int payloadSize() {
            return delegate.payloadSize();
        }

        @Override
        public int pageReservedBytes() {
            return delegate.pageReservedBytes();
        }

        @Override
        public long fileSize() throws IOException {
            return delegate.fileSize();
        }

        @Override
        public Path path() {
            return delegate.path();
        }

        @Override
        public void flushAndForce(FileFlushEvent flushEvent) throws IOException {
            delegate.flushAndForce(flushEvent);
            flushTicket.use();
        }

        @Override
        public long getLastPageId() throws IOException {
            return delegate.getLastPageId();
        }

        @Override
        public void increaseLastPageIdTo(long newLastPageId) {
            delegate.increaseLastPageIdTo(newLastPageId);
        }

        @Override
        public void close() {
            // Just like MuninnPagedFile#close() this method on a specific instance can be called multiple times,
            // or rather as many times as this file has been mapped (and not yet unmapped) in this database.
            unmap(this);
            delegate.close();
        }

        @Override
        public void setDeleteOnClose(boolean deleteOnClose) {
            delegate.setDeleteOnClose(deleteOnClose);
        }

        @Override
        public boolean isDeleteOnClose() {
            return delegate.isDeleteOnClose();
        }

        @Override
        public String getDatabaseName() {
            return delegate.getDatabaseName();
        }

        @Override
        public PageFileCounters pageFileCounters() {
            return delegate.pageFileCounters();
        }

        @Override
        public boolean isMultiVersioned() {
            return delegate.isMultiVersioned();
        }

        @Override
        public void truncate(long pagesToKeep, FileTruncateEvent fileTruncateEvent) throws IOException {
            delegate.truncate(pagesToKeep, fileTruncateEvent);
        }

        @Override
        public int touch(long pageId, int count, CursorContext cursorContext) throws IOException {
            return delegate.touch(pageId, count, cursorContext);
        }

        @Override
        public boolean preAllocateSupported() {
            return delegate.preAllocateSupported();
        }

        @Override
        public void preAllocate(long newFileSizeInPages) throws IOException {
            delegate.preAllocate(newFileSizeInPages);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            DatabasePagedFile that = (DatabasePagedFile) o;
            return delegate.equals(that.delegate);
        }

        @Override
        public int hashCode() {
            return Objects.hash(delegate);
        }

        Ticket flushTicket() {
            return flushTicket;
        }
    }
}
