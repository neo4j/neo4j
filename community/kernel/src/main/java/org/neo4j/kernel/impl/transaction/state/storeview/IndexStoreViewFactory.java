/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
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
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.kernel.impl.transaction.state.storeview;

import java.util.function.Function;
import java.util.function.Supplier;
import org.neo4j.configuration.Config;
import org.neo4j.io.pagecache.context.CursorContext;
import org.neo4j.kernel.impl.api.index.IndexStoreView;
import org.neo4j.kernel.impl.api.index.IndexingService.IndexProxyProvider;
import org.neo4j.kernel.impl.locking.Locks;
import org.neo4j.lock.LockService;
import org.neo4j.logging.InternalLogProvider;
import org.neo4j.storageengine.api.StorageReader;
import org.neo4j.storageengine.api.cursor.StoreCursors;

public class IndexStoreViewFactory {
    private final FullScanStoreView fullScanStoreView;
    private final Function<CursorContext, StoreCursors> cursorFactory;
    private final Locks locks;
    private final LockService lockService;
    private final Config config;
    private final Supplier<StorageReader> storageReader;
    private final InternalLogProvider logProvider;

    public IndexStoreViewFactory(
            Config config,
            Function<CursorContext, StoreCursors> cursorFactory,
            Supplier<StorageReader> storageReader,
            Locks locks,
            FullScanStoreView fullScanStoreView,
            LockService lockService,
            InternalLogProvider logProvider) {
        this.cursorFactory = cursorFactory;
        this.locks = locks;
        this.lockService = lockService;
        this.config = config;
        this.storageReader = storageReader;
        this.logProvider = logProvider;
        this.fullScanStoreView = fullScanStoreView;
    }

    public IndexStoreView createTokenIndexStoreView(IndexProxyProvider indexProxies) {
        return new DynamicIndexStoreView(
                fullScanStoreView, locks, lockService, config, indexProxies, storageReader, cursorFactory, logProvider);
    }
}
