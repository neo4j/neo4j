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

import static org.neo4j.index.internal.gbptree.ValueMerger.MergeResult.MERGED;
import static org.neo4j.index.internal.gbptree.ValueMerger.MergeResult.REMOVED;
import static org.neo4j.storageengine.api.TransactionIdStore.BASE_TX_ID;

import java.io.IOException;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import org.eclipse.collections.impl.list.mutable.FastList;
import org.neo4j.index.internal.gbptree.GBPTree;
import org.neo4j.index.internal.gbptree.ValueMerger;
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

    /**
     * {@link Comparator} for sorting the entity id ranges, used in batches to apply updates in sorted order.
     */
    private static final Comparator<TokenIndexEntryUpdate> UPDATE_SORTER =
            Comparator.comparingLong(TokenIndexEntryUpdate::getEntityId);

    /**
     * {@link ValueMerger} used for adding token->entity mappings, see {@link TokenScanValue#add(TokenScanValue)}.
     */
    private static final ValueMerger<TokenScanKey, TokenScanValue> ADD_MERGER =
            (existingKey, newKey, existingValue, newValue) -> {
                existingValue.add(newValue);
                return MERGED;
            };

    /**
     * {@link ValueMerger} used for removing token->entity mappings, see {@link TokenScanValue#remove(TokenScanValue)}.
     */
    private static final ValueMerger<TokenScanKey, TokenScanValue> REMOVE_MERGER =
            (existingKey, newKey, existingValue, newValue) -> {
                existingValue.remove(newValue);
                return existingValue.isEmpty() ? REMOVED : MERGED;
            };

    /**
     * {@link Writer} acquired when acquiring this {@link TokenIndexUpdater},
     * acquired from {@link GBPTree#writer(CursorContext)}.
     */
    private Writer<TokenScanKey, TokenScanValue> writer;

    /**
     * Instance of {@link TokenScanKey} acting as place to read keys into and to set for each applied update.
     */
    private final TokenScanKey key = new TokenScanKey();

    /**
     * Instance of {@link TokenScanValue} acting as place to read values into and to update
     * for each applied update.
     */
    private final TokenScanValue value = new TokenScanValue();

    /**
     * Batch currently building up as {@link #process(IndexEntryUpdate) updates} come in. Cursor for where
     * to place new updates is {@link #pendingUpdatesCursor}. The constructor set the length of this queue
     * and the length defines the maximum batch size.
     */
    private final TokenIndexEntryUpdate[] pendingUpdates;

    private final TokenIndexIdLayout idLayout;

    /**
     * Cursor into {@link #pendingUpdates}, where to place new {@link #process(IndexEntryUpdate) updates}.
     * When full the batch is applied and this cursor reset to {@code 0}.
     */
    private int pendingUpdatesCursor;

    /**
     * There are two levels of batching, one for {@link TokenIndexEntryUpdate updates} and one when applying.
     * This variable helps keeping track of the second level where updates to the actual {@link GBPTree}
     * are batched per entity id range, i.e. to add several tokenId->entityId mappings falling into the same
     * range, all of those updates are made into one {@link TokenScanValue} and then issues as one update
     * to the tree. There are additions and removals, this variable keeps track of which.
     */
    private boolean addition;

    /**
     * When applying {@link TokenIndexEntryUpdate updates} (when batch full or in {@link #close()}), updates are
     * applied tokenId by tokenId. All updates are scanned through multiple times, with one token in mind at a time.
     * For each round the current round tries to figure out which is the closest higher tokenId to apply
     * in the next round. This variable keeps track of that next tokenId.
     */
    private int lowestTokenId;

    private boolean closed = true;
    private boolean parallel;

    private CursorContext cursorContext;
    private CursorContext writerCursorContext;
    private long lastVersion;

    TokenIndexUpdater(int batchSize, TokenIndexIdLayout idLayout) {
        this.pendingUpdates = new TokenIndexEntryUpdate[batchSize];
        this.idLayout = idLayout;
    }

    TokenIndexUpdater initialize(TokenWriterFactory writerFactory, boolean parallel, CursorContext cursorContext)
            throws IOException {
        if (!closed) {
            throw new IllegalStateException("Updater still open");
        }
        this.cursorContext = cursorContext;
        this.writerCursorContext = cursorContext.createRelatedContext("TOKEN_INDEX_UPDATER_APPLY");
        this.writer = writerFactory.create(writerCursorContext);
        this.parallel = parallel;
        this.lastVersion = cursorContext.getVersionContext().committingTransactionId();
        this.pendingUpdatesCursor = 0;
        this.addition = false;
        this.lowestTokenId = Integer.MAX_VALUE;
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
        long committingTransactionId = cursorContext.getVersionContext().committingTransactionId();
        if (pendingUpdatesCursor == pendingUpdates.length || lastVersion != committingTransactionId) {
            flushPendingChanges(lastVersion);
        }
        lastVersion = committingTransactionId;
        TokenIndexEntryUpdate tokenUpdate = asTokenUpdate(update);
        pendingUpdates[pendingUpdatesCursor++] = tokenUpdate;
        checkNextTokenId(tokenUpdate.removed());
        checkNextTokenId(tokenUpdate.added());
    }

    @Override
    public void yield() {
        writer.yield();
    }

    private void checkNextTokenId(int[] tokens) {
        if (tokens.length > 0) {
            lowestTokenId = Math.min(lowestTokenId, tokens[0]);
        }
    }

    private void flushPendingChanges(long version) {
        if (pendingUpdatesCursor == 0) {
            return;
        }
        if (version >= BASE_TX_ID) {
            // this updater can be called from popuplating thread with no version context, this is expected, and we
            // don't need to reset writer version in this case
            writerCursorContext.getVersionContext().initWrite(version);
        } else {
            assert !writerCursorContext.getVersionContext().initializedForWrite();
        }
        Arrays.sort(pendingUpdates, 0, pendingUpdatesCursor, UPDATE_SORTER);
        int currentTokenId = lowestTokenId;
        value.clear();
        key.clear();
        List<Change> changes = parallel ? FastList.newList() : null;
        while (currentTokenId != Integer.MAX_VALUE) {
            int nextTokenId = Integer.MAX_VALUE;
            for (int i = 0; i < pendingUpdatesCursor; i++) {
                TokenIndexEntryUpdate update = pendingUpdates[i];
                long entityId = update.getEntityId();
                nextTokenId = extractChange(update.added(), currentTokenId, entityId, nextTokenId, true, changes);
                nextTokenId = extractChange(update.removed(), currentTokenId, entityId, nextTokenId, false, changes);
            }
            currentTokenId = nextTokenId;
        }
        flushPendingRange(changes);
        pendingUpdatesCursor = 0;

        if (changes != null) {
            for (Change change : changes) {
                writeChange(change.key, change.value, change.addition);
            }
            writer.yield();
        }
    }

    private void writeChange(TokenScanKey key, TokenScanValue value, boolean addition) {
        if (addition) {
            writer.merge(key, value, ADD_MERGER);
        } else {
            writer.mergeIfExists(key, value, REMOVE_MERGER);
        }
    }

    private int extractChange(
            int[] tokens, int currentTokenId, long entityId, int nextTokenId, boolean addition, List<Change> changes) {
        int foundNextTokenId = nextTokenId;
        for (int li = 0; li < tokens.length; li++) {
            int tokenId = tokens[li];

            // Have this check here so that we can pick up the next tokenId in our change set
            if (tokenId == currentTokenId) {
                change(currentTokenId, entityId, addition, changes);

                // We can do a little shorter check for next tokenId here straight away,
                // we just check the next if it's less than what we currently think is next tokenId
                // and then break right after
                if (li + 1 < tokens.length) {
                    int nextTokenCandidate = tokens[li + 1];
                    if (nextTokenCandidate < currentTokenId) {
                        throw new IllegalArgumentException(
                                "The entity token contained unsorted tokens ids " + Arrays.toString(tokens));
                    }
                    if (nextTokenCandidate > currentTokenId) {
                        foundNextTokenId = Math.min(foundNextTokenId, nextTokenCandidate);
                    }
                }
                break;
            } else if (tokenId > currentTokenId) {
                foundNextTokenId = Math.min(foundNextTokenId, tokenId);
            }
        }
        return foundNextTokenId;
    }

    private void change(int tokenId, long entityId, boolean add, List<Change> changes) {
        long idRange = idLayout.rangeOf(entityId);
        if (tokenId != key.tokenId || idRange != key.idRange || addition != add) {
            flushPendingRange(changes);

            // Set key to current and reset value
            key.tokenId = tokenId;
            key.idRange = idRange;
            addition = add;
        }

        int offset = idLayout.idWithinRange(entityId);
        value.set(offset);
    }

    private void flushPendingRange(List<Change> changes) {
        if (value.bits != 0) {
            // There are changes in the current range, flush them
            if (parallel) {
                changes.add(new Change(
                        new TokenScanKey(key.tokenId, key.idRange), new TokenScanValue(value.bits), addition));
            } else {
                writeChange(key, value, addition);
            }
            value.clear();
        }
    }

    /**
     * Applies {@link #process(IndexEntryUpdate) queued updates} which has not yet been applied.
     * No more {@link #process(IndexEntryUpdate) updates} can be applied after this call.
     */
    @Override
    public void close() {
        try {
            flushPendingChanges(lastVersion);
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

    record Change(TokenScanKey key, TokenScanValue value, boolean addition) {}

    @FunctionalInterface
    interface TokenWriterFactory {
        Writer<TokenScanKey, TokenScanValue> create(CursorContext cursorContext) throws IOException;
    }
}
