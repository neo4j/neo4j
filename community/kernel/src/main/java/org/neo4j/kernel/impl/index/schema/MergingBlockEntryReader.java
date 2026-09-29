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

import static org.neo4j.io.IOUtils.closeAll;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.neo4j.index.internal.gbptree.Layout;
import org.neo4j.util.Preconditions;

/**
 * Take multiple {@link BlockEntryCursor} that each by themselves provide block entries in sorted order and lazily merge join, providing a view over all
 * entries from given cursors in sorted order.
 * Merging is done with a tournament (loser) tree over the sources, so selecting the next entry costs {@code ceil(log2(sources))} key comparisons
 * regardless of how many sources there are. Ties are broken towards the source that was added first.
 * Instances handed out from {@link #key()} and {@link #value()} are reused, consumer is responsible for creating copy if there is a need to cache results.
 */
public class MergingBlockEntryReader<KEY, VALUE> implements BlockEntryMerger<KEY, VALUE> {
    private final Layout<KEY, VALUE> layout;
    private final List<BlockEntryCursor<KEY, VALUE>> sources = new ArrayList<>();

    // Tournament tree state, built on the first call to next(). Leaf i is source i. Internal node t (1 <= t < k) holds
    // the index of the
    // source that lost the match played at that node, tree[0] holds the overall winner. A source index of k is the
    // initial sentinel, which
    // compares lower than everything so that the tree can be built by simply playing every leaf up towards the root.
    private BlockEntryCursor<KEY, VALUE>[] cursors;
    private KEY[] heads; // current head key of each source, or null if that source is exhausted
    private int[] tree;
    private int winner;
    private boolean initialized;

    MergingBlockEntryReader(Layout<KEY, VALUE> layout) {
        this.layout = layout;
    }

    @Override
    public void addSource(BlockEntryCursor<KEY, VALUE> source) {
        Preconditions.checkState(!initialized, "Cannot add sources after merging has started");
        sources.add(source);
    }

    @Override
    public boolean next() throws IOException {
        if (!initialized) {
            initialize();
        } else {
            if (heads[winner] == null) {
                // All sources exhausted, this was already reported on the previous call
                return false;
            }
            advance(winner);
            replay(winner);
        }
        winner = tree[0];
        return heads[winner] != null;
    }

    @Override
    public KEY key() {
        return cursors[winner].key();
    }

    @Override
    public VALUE value() {
        return cursors[winner].value();
    }

    @Override
    public void close() throws IOException {
        closeAll(sources);
    }

    @SuppressWarnings("unchecked")
    private void initialize() throws IOException {
        Preconditions.checkState(!sources.isEmpty(), "Cannot merge without any sources");
        initialized = true;
        int k = sources.size();
        cursors = sources.toArray(new BlockEntryCursor[k]);
        heads = (KEY[]) new Object[k];
        tree = new int[k];
        for (int i = 0; i < k; i++) {
            advance(i);
        }
        Arrays.fill(tree, k);
        for (int i = k - 1; i >= 0; i--) {
            replay(i);
        }
    }

    private void advance(int source) throws IOException {
        BlockEntryCursor<KEY, VALUE> cursor = cursors[source];
        heads[source] = cursor.next() ? cursor.key() : null;
    }

    /**
     * Plays source {@code s} up the tree from its leaf: at every internal node the loser stays and the winner continues upwards,
     * ending with the overall winner at the root.
     */
    private void replay(int s) {
        int k = cursors.length;
        for (int t = (s + k) >>> 1; t > 0; t >>>= 1) {
            int other = tree[t];
            if (wins(other, s)) {
                tree[t] = s;
                s = other;
            }
        }
        tree[0] = s;
    }

    /**
     * @return whether source {@code a} is selected before source {@code b}. The sentinel (index k) beats everything so that it gets
     * pushed up and out of the tree during initialization, an exhausted source (null head) loses against everything.
     */
    private boolean wins(int a, int b) {
        int k = cursors.length;
        if (a == k) {
            return true;
        }
        if (b == k) {
            return false;
        }
        KEY headA = heads[a];
        KEY headB = heads[b];
        if (headA == null) {
            return false;
        }
        if (headB == null) {
            return true;
        }
        int comparison = layout.compare(headA, headB);
        return comparison < 0 || (comparison == 0 && a < b);
    }
}
