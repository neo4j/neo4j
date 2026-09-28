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

import static org.neo4j.index.internal.gbptree.DataTree.W_BATCHED_SINGLE_THREADED;

import java.io.IOException;
import java.util.Collection;
import java.util.function.IntFunction;
import org.neo4j.common.EntityType;
import org.neo4j.index.internal.gbptree.GBPTree;
import org.neo4j.index.internal.gbptree.Layout;
import org.neo4j.index.internal.gbptree.Seeker;
import org.neo4j.internal.schema.IndexOrder;
import org.neo4j.io.pagecache.context.CursorContext;
import org.neo4j.kernel.api.index.EntityRange;
import org.neo4j.kernel.api.index.IndexProgressor;
import org.neo4j.kernel.api.index.TokenIndexProgressor;
import org.neo4j.storageengine.api.IndexEntryUpdate;

/**
 * The token index format with 64 entities per tree value, using {@link TokenScanLayout}.
 */
public final class BasicTokenIndexFormat implements TokenIndexFormat<TokenScanValue> {
    public static final BasicTokenIndexFormat INSTANCE = new BasicTokenIndexFormat(new DefaultTokenIndexIdLayout());

    private static final int BATCH_SIZE = 1_000;

    private final TokenIndexIdLayout idLayout;
    private final TokenScanLayout treeLayout = new TokenScanLayout();

    public BasicTokenIndexFormat(TokenIndexIdLayout idLayout) {
        this.idLayout = idLayout;
    }

    @Override
    public TokenIndexIdLayout idLayout() {
        return idLayout;
    }

    @Override
    public Layout<TokenScanKey, TokenScanValue> treeLayout() {
        return treeLayout;
    }

    @Override
    public SingleUpdater<TokenScanValue> newSingleUpdater() {
        return new BasicSingleUpdater(new TokenIndexUpdater(BATCH_SIZE, idLayout));
    }

    @Override
    public Updater openParallelUpdater(
            GBPTree<TokenScanKey, TokenScanValue> tree, int writerFlags, CursorContext cursorContext)
            throws IOException {
        return new TokenIndexUpdater(BATCH_SIZE, idLayout)
                .initialize(context -> tree.writer(writerFlags, context), true, cursorContext);
    }

    @Override
    public TokenIndexProgressor newProgressor(
            Seeker<TokenScanKey, TokenScanValue> seeker,
            IndexProgressor.EntityTokenClient client,
            IndexOrder indexOrder,
            EntityRange range,
            int tokenId) {
        return TokenScanValueIndexProgressor.create(seeker, client, indexOrder, range, idLayout, tokenId);
    }

    @Override
    public AllEntriesTokenScanReader newAllEntriesReader(
            IntFunction<Seeker<TokenScanKey, TokenScanValue>> seekProvider, int highestTokenId, EntityType entityType) {
        return new NativeAllEntriesTokenScanReader(seekProvider, highestTokenId, entityType, idLayout);
    }

    private record BasicSingleUpdater(TokenIndexUpdater updater) implements SingleUpdater<TokenScanValue> {
        @Override
        public Updater open(GBPTree<TokenScanKey, TokenScanValue> tree, CursorContext cursorContext)
                throws IOException {
            return updater.initialize(context -> tree.writer(W_BATCHED_SINGLE_THREADED, context), false, cursorContext);
        }

        @Override
        public void addAll(
                GBPTree<TokenScanKey, TokenScanValue> tree,
                Collection<? extends IndexEntryUpdate> updates,
                CursorContext cursorContext)
                throws IOException {
            try (TokenIndexUpdater opened = updater.initialize(
                    context -> tree.writer(W_BATCHED_SINGLE_THREADED, context), false, cursorContext)) {
                for (IndexEntryUpdate update : updates) {
                    opened.process(update);
                }
            }
        }
    }
}
