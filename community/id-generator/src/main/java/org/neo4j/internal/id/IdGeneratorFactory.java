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
import java.util.function.Consumer;
import java.util.function.LongSupplier;
import org.eclipse.collections.api.set.ImmutableSet;
import org.neo4j.configuration.Config;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.context.CursorContext;
import org.neo4j.io.pagecache.context.CursorContextFactory;
import org.neo4j.io.pagecache.impl.muninn.StoreFile;

public interface IdGeneratorFactory extends IdGeneratorSupplier {
    IdGenerator open(
            PageCache pageCache,
            StoreFile storeFile,
            IdType idType,
            LongSupplier highIdScanner,
            long maxId,
            boolean readOnly,
            Config config,
            CursorContextFactory contextFactory,
            ImmutableSet<OpenOption> openOptions,
            IdSlotDistribution slotDistribution)
            throws IOException;

    IdGenerator create(
            PageCache pageCache,
            StoreFile storeFile,
            IdType idType,
            long highId,
            boolean throwIfFileExists,
            long maxId,
            boolean readOnly,
            Config config,
            CursorContextFactory contextFactory,
            ImmutableSet<OpenOption> openOptions,
            IdSlotDistribution slotDistribution)
            throws IOException;

    void visit(Consumer<IdGenerator> visitor);

    void clearCache(boolean allocationEnabled, CursorContext cursorContext);

    default void notifyTransactionRollback(long transactionId) {}
}
