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
import org.neo4j.kernel.api.index.IndexUpdater;
import org.neo4j.kernel.api.index.TokenIndexProgressor;
import org.neo4j.storageengine.api.IndexEntryUpdate;

/**
 * The format of the token index. It supplies the id layout, the tree layout, and the updaters, progressors and all-entries readers working on values of that layout.
 *
 * @param <VALUE> type of the tree values
 */
public interface TokenIndexFormat<VALUE> {
    TokenIndexIdLayout idLayout();

    Layout<TokenScanKey, VALUE> treeLayout();

    SingleUpdater<VALUE> newSingleUpdater();

    Updater openParallelUpdater(GBPTree<TokenScanKey, VALUE> tree, int writerFlags, CursorContext cursorContext)
            throws IOException;

    TokenIndexProgressor newProgressor(
            Seeker<TokenScanKey, VALUE> seeker,
            IndexProgressor.EntityTokenClient client,
            IndexOrder indexOrder,
            EntityRange range,
            int tokenId);

    AllEntriesTokenScanReader newAllEntriesReader(
            IntFunction<Seeker<TokenScanKey, VALUE>> seekProvider, int highestTokenId, EntityType entityType);

    /**
     * Exception free IndexUpdater.
     */
    interface Updater extends IndexUpdater {
        @Override
        void process(IndexEntryUpdate update);

        @Override
        void close();
    }

    interface SingleUpdater<VALUE> {
        Updater open(GBPTree<TokenScanKey, VALUE> tree, CursorContext cursorContext) throws IOException;

        void addAll(
                GBPTree<TokenScanKey, VALUE> tree,
                Collection<? extends IndexEntryUpdate> updates,
                CursorContext cursorContext)
                throws IOException;
    }
}
