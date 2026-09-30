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

import java.io.IOException;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import org.eclipse.collections.impl.list.mutable.FastList;
import org.neo4j.index.internal.gbptree.GBPTree;
import org.neo4j.index.internal.gbptree.ValueMerger;
import org.neo4j.index.internal.gbptree.Writer;
import org.neo4j.io.pagecache.context.CursorContext;
import org.neo4j.storageengine.api.TokenIndexEntryUpdate;

/**
 * Batch of {@link TokenIndexEntryUpdate updates} queued up by a token index updater, applied to its {@link GBPTree}
 * in sorted order (by token and entity id) on {@link #flush(Writer, boolean)}.
 */
final class TokenIndexUpdateBatch<V extends TokenIndexValue<V>> {
    private static final Comparator<TokenIndexEntryUpdate> UPDATE_SORTER =
            Comparator.comparingLong(TokenIndexEntryUpdate::getEntityId);

    private final ValueMerger<TokenScanKey, V> addMerger = (existingKey, newKey, existingValue, newValue) -> {
        existingValue.add(newValue);
        return MERGED;
    };

    private final ValueMerger<TokenScanKey, V> removeMerger = (existingKey, newKey, existingValue, newValue) -> {
        existingValue.remove(newValue);
        return existingValue.isEmpty() ? REMOVED : MERGED;
    };

    private final TokenIndexEntryUpdate[] pendingUpdates;
    private final TokenIndexIdLayout idLayout;
    private final TokenScanKey key = new TokenScanKey();

    /**
     * Changes of the current id range, for the token in {@link #key}, are gathered here and issued as one update
     * to the tree.
     */
    private final V value;

    private int pendingUpdatesCursor;

    /**
     * Whether the id range currently being built up in {@link #value} is an addition or a removal.
     */
    private boolean addition;

    /**
     * Lowest token id among the pending updates, where applying a batch starts. Each round over the batch
     * finds the next higher token id to apply.
     */
    private int lowestTokenId = Integer.MAX_VALUE;

    private Writer<TokenScanKey, V> writer;

    /**
     * Changes collected for writing after the whole batch is processed, if flushing a parallel writer.
     */
    private List<Change<V>> changes;

    TokenIndexUpdateBatch(int batchSize, TokenIndexIdLayout idLayout, V value) {
        this.pendingUpdates = new TokenIndexEntryUpdate[batchSize];
        this.idLayout = idLayout;
        this.value = value;
    }

    void clear() {
        pendingUpdatesCursor = 0;
        lowestTokenId = Integer.MAX_VALUE;
    }

    boolean isEmpty() {
        return pendingUpdatesCursor == 0;
    }

    boolean isFull() {
        return pendingUpdatesCursor == pendingUpdates.length;
    }

    void add(TokenIndexEntryUpdate update) {
        pendingUpdates[pendingUpdatesCursor++] = update;
        checkNextTokenId(update.removed());
        checkNextTokenId(update.added());
    }

    private void checkNextTokenId(int[] tokens) {
        if (tokens.length > 0) {
            lowestTokenId = Math.min(lowestTokenId, tokens[0]);
        }
    }

    /**
     * Applies all pending updates through {@code writer} and clears the batch. A {@code parallel} writer is yielded
     * afterwards.
     */
    void flush(Writer<TokenScanKey, V> writer, boolean parallel) {
        if (isEmpty()) {
            return;
        }
        this.writer = writer;
        this.changes = parallel ? FastList.newList() : null;
        try {
            Arrays.sort(pendingUpdates, 0, pendingUpdatesCursor, UPDATE_SORTER);
            int currentTokenId = lowestTokenId;
            value.clear();
            key.clear();
            while (currentTokenId != Integer.MAX_VALUE) {
                int nextTokenId = Integer.MAX_VALUE;
                for (int i = 0; i < pendingUpdatesCursor; i++) {
                    TokenIndexEntryUpdate update = pendingUpdates[i];
                    long entityId = update.getEntityId();
                    nextTokenId = extractChange(update.added(), currentTokenId, entityId, nextTokenId, true);
                    nextTokenId = extractChange(update.removed(), currentTokenId, entityId, nextTokenId, false);
                }
                currentTokenId = nextTokenId;
            }
            flushPendingRange();
            clear();

            if (changes != null) {
                for (Change<V> change : changes) {
                    writeChange(change.key, change.value, change.addition);
                }
                writer.yield();
            }
        } finally {
            this.writer = null;
            this.changes = null;
        }
    }

    private int extractChange(int[] tokens, int currentTokenId, long entityId, int nextTokenId, boolean addition) {
        int foundNextTokenId = nextTokenId;
        for (int li = 0; li < tokens.length; li++) {
            int tokenId = tokens[li];

            // Have this check here so that we can pick up the next tokenId in our change set
            if (tokenId == currentTokenId) {
                change(currentTokenId, entityId, addition);

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

    private void change(int tokenId, long entityId, boolean add) {
        long idRange = idLayout.rangeOf(entityId);
        if (tokenId != key.tokenId || idRange != key.idRange || addition != add) {
            flushPendingRange();

            key.tokenId = tokenId;
            key.idRange = idRange;
            addition = add;
        }

        value.set(idLayout.idWithinRange(entityId));
    }

    private void flushPendingRange() {
        if (!value.isEmpty()) {
            if (changes != null) {
                changes.add(new Change<>(new TokenScanKey(key.tokenId, key.idRange), value.copy(), addition));
            } else {
                writeChange(key, value, addition);
            }
            value.clear();
        }
    }

    private void writeChange(TokenScanKey key, V value, boolean addition) {
        if (addition) {
            writer.merge(key, value, addMerger);
        } else {
            writer.mergeIfExists(key, value, removeMerger);
        }
    }

    private record Change<V>(TokenScanKey key, V value, boolean addition) {}

    @FunctionalInterface
    interface WriterFactory<V> {
        Writer<TokenScanKey, V> create(CursorContext cursorContext) throws IOException;
    }
}
