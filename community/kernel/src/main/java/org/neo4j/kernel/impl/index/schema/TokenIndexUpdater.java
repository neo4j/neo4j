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

import java.io.IOException;
import org.neo4j.index.internal.gbptree.GBPTree;
import org.neo4j.index.internal.gbptree.Writer;
import org.neo4j.io.IOUtils;
import org.neo4j.io.pagecache.context.CursorContext;
import org.neo4j.kernel.api.index.IndexUpdater;
import org.neo4j.kernel.api.index.TokenIndexReader;
import org.neo4j.storageengine.api.IndexEntryUpdate;
import org.neo4j.storageengine.api.TokenIndexEntryUpdate;

/**
 * {@link IndexUpdater} for token index, or rather a {@link Writer} for its internal {@link GBPTree}.
 * <p>
 * {@link #process(IndexEntryUpdate) updates} are queued up to a maximum batch size and, for performance,
 * applied in sorted order (by the token and entity id) when reaches batch size or on {@link #close()}.
 * <p>
 * Updates aren't visible to {@link TokenIndexReader readers} immediately, rather when queue happens to be applied.
 */
class TokenIndexUpdater implements TokenIndexFormat.Updater {
    private final TokenIndexUpdateBatch<TokenScanValue> batch;

    /**
     * {@link Writer} acquired when acquiring this {@link TokenIndexUpdater},
     * acquired from {@link GBPTree#writer(CursorContext)}.
     */
    private Writer<TokenScanKey, TokenScanValue> writer;

    private boolean closed = true;
    private boolean parallel;

    TokenIndexUpdater(int batchSize, TokenIndexIdLayout idLayout) {
        this.batch = new TokenIndexUpdateBatch<>(batchSize, idLayout, new TokenScanValue());
    }

    TokenIndexUpdater initialize(
            TokenIndexUpdateBatch.WriterFactory<TokenScanValue> writerFactory,
            boolean parallel,
            CursorContext cursorContext)
            throws IOException {
        if (!closed) {
            throw new IllegalStateException("Updater still open");
        }
        this.writer = writerFactory.create(cursorContext);
        this.parallel = parallel;
        this.batch.clear();
        this.closed = false;
        return this;
    }

    /**
     * Queues a {@link TokenIndexEntryUpdate} to this writer for applying when batch gets full,
     * or when {@link #close() closing}.
     *
     * Calls to this method MUST be ordered by ascending entity id.
     */
    @Override
    public void process(IndexEntryUpdate update) {
        assertOpen();
        if (batch.isFull()) {
            batch.flush(writer, parallel);
        }
        batch.add(asTokenUpdate(update));
    }

    @Override
    public void yield() {
        writer.yield();
    }

    /**
     * Applies {@link #process(IndexEntryUpdate) queued updates} which has not yet been applied.
     * No more {@link #process(IndexEntryUpdate) updates} can be applied after this call.
     */
    @Override
    public void close() {
        try {
            batch.flush(writer, parallel);
        } finally {
            closed = true;
            IOUtils.closeAllUnchecked(writer);
        }
    }

    private void assertOpen() {
        if (closed) {
            throw new IllegalStateException("Updater has been closed");
        }
    }
}
