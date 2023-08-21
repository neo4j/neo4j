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
package org.neo4j.adversaries.pagecache;

import java.io.IOException;
import java.nio.file.NoSuchFileException;
import java.util.Objects;
import org.neo4j.adversaries.Adversary;
import org.neo4j.io.pagecache.DelegatingPagedFile;
import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.io.pagecache.PagedFile;
import org.neo4j.io.pagecache.context.CursorContext;
import org.neo4j.io.pagecache.tracing.FileFlushEvent;
import org.neo4j.io.pagecache.tracing.version.FileTruncateEvent;

/**
 * A {@linkplain PagedFile paged file} that wraps another paged file and an {@linkplain Adversary adversary} to provide
 * a misbehaving paged file implementation for testing.
 * <p>
 * Depending on the adversary each operation can throw either {@link RuntimeException} like {@link SecurityException}
 * or {@link IOException} like {@link NoSuchFileException}.
 */
@SuppressWarnings("unchecked")
public class AdversarialPagedFile extends DelegatingPagedFile {
    private final Adversary adversary;

    public AdversarialPagedFile(PagedFile delegate, Adversary adversary) {
        super(delegate);
        Objects.requireNonNull(delegate);
        this.adversary = Objects.requireNonNull(adversary);
    }

    @Override
    public PageCursor io(long pageId, int pf_flags, CursorContext context) throws IOException {
        adversary.injectFailure(IllegalStateException.class);
        PageCursor pageCursor = delegate.io(pageId, pf_flags, context);
        if ((pf_flags & PF_SHARED_READ_LOCK) == PF_SHARED_READ_LOCK) {
            return new AdversarialReadPageCursor(pageCursor, adversary);
        }
        return new AdversarialWritePageCursor(pageCursor, adversary);
    }

    @Override
    public long fileSize() throws IOException {
        adversary.injectFailure(IllegalStateException.class);
        return delegate.fileSize();
    }

    @Override
    public void flushAndForce(FileFlushEvent flushEvent) throws IOException {
        adversary.injectFailure(NoSuchFileException.class, IOException.class, SecurityException.class);
        delegate.flushAndForce(flushEvent);
    }

    @Override
    public long getLastPageId() throws IOException {
        adversary.injectFailure(IllegalStateException.class);
        return delegate.getLastPageId();
    }

    @Override
    public void close() {
        adversary.injectFailure(NoSuchFileException.class, SecurityException.class);
        delegate.close();
    }

    @Override
    public void truncate(long pagesToKeep, FileTruncateEvent fileTruncateEvent) throws IOException {
        adversary.injectFailure(IOException.class);
        delegate.truncate(pagesToKeep, fileTruncateEvent);
    }

    @Override
    public int touch(long pageId, int count, CursorContext cursorContext) throws IOException {
        adversary.injectFailure(IOException.class);
        return delegate.touch(pageId, count, cursorContext);
    }

    @Override
    public String toString() {
        return "AdversarialPagedFile{" + "delegate=" + delegate + '}';
    }
}
