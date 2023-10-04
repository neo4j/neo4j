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

import static java.nio.file.StandardOpenOption.CREATE;

import java.io.IOException;
import java.nio.file.NoSuchFileException;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import org.eclipse.collections.api.set.ImmutableSet;
import org.neo4j.adversaries.Adversary;
import org.neo4j.io.pagecache.IOController;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.PagedFile;
import org.neo4j.io.pagecache.buffer.IOBufferFactory;
import org.neo4j.io.pagecache.impl.muninn.EvictionBouncer;
import org.neo4j.io.pagecache.impl.muninn.VersionStorage;
import org.neo4j.io.pagecache.tracing.DatabaseFlushEvent;
import org.neo4j.util.VisibleForTesting;

/**
 * A {@linkplain PageCache page cache} that wraps another page cache and an {@linkplain Adversary adversary} to provide
 * a misbehaving page cache implementation for testing.
 * <p>
 * Depending on the adversary each operation can throw either {@link RuntimeException} like {@link SecurityException}
 * or {@link IOException} like {@link NoSuchFileException}.
 */
@SuppressWarnings("unchecked")
public class AdversarialPageCache implements PageCache {
    private final PageCache delegate;
    private final Adversary adversary;

    public AdversarialPageCache(PageCache delegate, Adversary adversary) {
        this.delegate = Objects.requireNonNull(delegate);
        this.adversary = Objects.requireNonNull(adversary);
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
        if (openOptions.contains(CREATE)) {
            adversary.injectFailure(IOException.class, SecurityException.class);
        } else {
            adversary.injectFailure(NoSuchFileException.class, IOException.class, SecurityException.class);
        }
        PagedFile pagedFile =
                delegate.map(path, pageSize, databaseName, openOptions, ioController, evictionBouncer, versionStorage);
        return new AdversarialPagedFile(pagedFile, adversary);
    }

    @Override
    public Optional<PagedFile> getExistingMapping(Path path) throws IOException {
        adversary.injectFailure(IOException.class, SecurityException.class);
        final Optional<PagedFile> optional = delegate.getExistingMapping(path);
        return optional.map(pagedFile -> new AdversarialPagedFile(pagedFile, adversary));
    }

    @Override
    public List<PagedFile> listExistingMappings() throws IOException {
        adversary.injectFailure(IOException.class, SecurityException.class);
        return delegate.listExistingMappings().stream()
                .map(file -> new AdversarialPagedFile(file, adversary))
                .map(PagedFile.class::cast)
                .toList();
    }

    @Override
    public void flushAndForce(DatabaseFlushEvent flushEvent) throws IOException {
        adversary.injectFailure(NoSuchFileException.class, IOException.class, SecurityException.class);
        delegate.flushAndForce(flushEvent);
    }

    @Override
    public void close() {
        adversary.injectFailure(IllegalStateException.class);
        delegate.close();
    }

    @Override
    public int pageSize() {
        return delegate.pageSize();
    }

    @Override
    public int pageReservedBytes(ImmutableSet<OpenOption> openOptions) {
        return delegate.pageReservedBytes(openOptions);
    }

    @Override
    public long maxCachedPages() {
        return delegate.maxCachedPages();
    }

    @Override
    public IOBufferFactory getBufferFactory() {
        return delegate.getBufferFactory();
    }

    @VisibleForTesting
    public Adversary adversary() {
        return adversary;
    }
}
