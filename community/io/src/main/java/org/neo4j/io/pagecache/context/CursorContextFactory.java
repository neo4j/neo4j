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
package org.neo4j.io.pagecache.context;

import static org.neo4j.io.pagecache.context.FixedVersionContextSupplier.EMPTY_CONTEXT_SUPPLIER;

import org.neo4j.io.pagecache.tracing.PageCacheTracer;
import org.neo4j.io.pagecache.tracing.cursor.PageCursorTracer;

public class CursorContextFactory {
    public static final CursorContextFactory NULL_CONTEXT_FACTORY =
            new CursorContextFactory(PageCacheTracer.NULL, EMPTY_CONTEXT_SUPPLIER);
    private final PageCacheTracer cacheTracer;
    private final VersionContextSupplier versionContextSupplier;

    public CursorContextFactory(PageCacheTracer cacheTracer, VersionContextSupplier versionContextSupplier) {
        this.cacheTracer = cacheTracer;
        this.versionContextSupplier = versionContextSupplier;
    }

    public CursorContext create(String tag) {
        return new CursorContext(
                this, cacheTracer.createPageCursorTracer(tag), versionContextSupplier.createVersionContext());
    }

    public CursorContext create(String tag, VersionContext versionContext) {
        return new CursorContext(this, cacheTracer.createPageCursorTracer(tag), versionContext);
    }

    public CursorContext create(PageCursorTracer cursorTracer) {
        return new CursorContext(this, cursorTracer, versionContextSupplier.createVersionContext());
    }

    public void init(
            TransactionIdSnapshotFactory transactionIdSnapshotFactory,
            OldestTransactionIdFactory oldestTransactionIdFactory) {
        versionContextSupplier.init(transactionIdSnapshotFactory, oldestTransactionIdFactory);
    }
}
