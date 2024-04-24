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

import java.io.IOException;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.util.Collection;
import java.util.function.LongSupplier;
import java.util.function.Supplier;
import org.eclipse.collections.api.set.ImmutableSet;
import org.neo4j.configuration.Config;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.context.CursorContext;
import org.neo4j.io.pagecache.context.CursorContextFactory;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;
import org.neo4j.memory.MemoryTracker;

public abstract class AbstractBufferingIdGeneratorFactory extends LifecycleAdapter implements IdGeneratorFactory {
    protected final IdGeneratorFactory delegate;

    public AbstractBufferingIdGeneratorFactory(IdGeneratorFactory delegate) {
        this.delegate = delegate;
    }

    public abstract void initialize(
            FileSystemAbstraction fs,
            Path bufferBasePath,
            Config config,
            Supplier<IdController.TransactionSnapshot> snapshotSupplier,
            IdController.IdFreeCondition condition,
            MemoryTracker memoryTracker)
            throws IOException;

    @Override
    public IdGenerator open(
            PageCache pageCache,
            Path filename,
            IdType idType,
            LongSupplier highIdScanner,
            long maxId,
            boolean readOnly,
            Config config,
            CursorContextFactory contextFactory,
            ImmutableSet<OpenOption> openOptions,
            IdSlotDistribution slotDistribution)
            throws IOException {
        IdGenerator generator = delegate.open(
                pageCache,
                filename,
                idType,
                highIdScanner,
                maxId,
                readOnly,
                config,
                contextFactory,
                openOptions,
                slotDistribution);
        return wrapAndKeep(idType, generator);
    }

    @Override
    public IdGenerator create(
            PageCache pageCache,
            Path filename,
            IdType idType,
            long highId,
            boolean throwIfFileExists,
            long maxId,
            boolean readOnly,
            Config config,
            CursorContextFactory contextFactory,
            ImmutableSet<OpenOption> openOptions,
            IdSlotDistribution slotDistribution)
            throws IOException {
        IdGenerator idGenerator = delegate.create(
                pageCache,
                filename,
                idType,
                highId,
                throwIfFileExists,
                maxId,
                readOnly,
                config,
                contextFactory,
                openOptions,
                slotDistribution);
        return wrapAndKeep(idType, idGenerator);
    }

    @Override
    public Collection<Path> listIdFiles() {
        return delegate.listIdFiles();
    }

    protected abstract IdGenerator wrapAndKeep(IdType idType, IdGenerator generator);

    public abstract void maintenance(CursorContext cursorContext);

    @Override
    public void clear() {
        delegate.clear();
    }
}
